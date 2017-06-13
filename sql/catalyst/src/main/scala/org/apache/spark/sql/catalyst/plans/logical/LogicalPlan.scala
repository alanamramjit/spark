/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types.StructType


abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

  private var _analyzed: Boolean = false

  /**
   * Marks this plan as already analyzed.  This should only be called by CheckAnalysis.
   */
  private[catalyst] def setAnalyzed(): Unit = { _analyzed = true }

  /**
   * Returns true if this node and its children have already been gone through analysis and
   * verification.  Note that this is only an optimization used to avoid analyzing trees that
   * have already been analyzed, and can be reset by transformations.
   */
  def analyzed: Boolean = _analyzed

  /** Returns true if this subtree contains any streaming data sources. */
  def isStreaming: Boolean = children.exists(_.isStreaming == true)

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
   * been marked as analyzed.
   *
   * @param rule the function use to transform this nodes children
   */
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {
      val afterRuleOnChildren = transformChildren(rule, (t, r) => t.resolveOperators(r))
      if (this fastEquals afterRuleOnChildren) {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(this, identity[LogicalPlan])
        }
      } else {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
        }
      }
    } else {
      this
    }
  }

  /**
   * Recursively transforms the expressions of a tree, skipping nodes that have already
   * been analyzed.
   */
  def resolveExpressions(r: PartialFunction[Expression, Expression]): LogicalPlan = {
    this resolveOperators  {
      case p => p.transformExpressions(r)
    }
  }

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  def statistics: Statistics = {
    if (children.isEmpty) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.statistics.sizeInBytes).product)
  }

  /**
   * Returns the maximum number of rows that this plan may compute.
   *
   * Any operator that a Limit can be pushed passed should override this function (e.g., Union).
   * Any operator that can push through a Limit should override this function (e.g., Project).
   */
  def maxRows: Option[Long] = None

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  override lazy val canonicalized: LogicalPlan = EliminateSubqueryAliases(this)

  /**
   * Resolves a given schema to concrete [[Attribute]] references in this query plan. This function
   * should only be called on analyzed plans since it will throw [[AnalysisException]] for
   * unresolved [[Attribute]]s.
   */
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case other => sys.error(s"can not handle nested schema yet...  plan $this")
      }.getOrElse {
        throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${output.map(_.name).mkString(", ")}]")
      }
    }
  }

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, children.flatMap(_.output), resolver)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, output, resolver)

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), output, resolver)
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * This assumes `name` has multiple parts, where the 1st part is a qualifier
   * (i.e. table name, alias, or subquery alias).
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsTableColumn(
      nameParts: Seq[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    if (attribute.qualifier.exists(resolver(_, nameParts.head))) {
      // At least one qualifier matches. See if remaining parts match.
      val remainingParts = nameParts.tail
      resolveAsColumn(remainingParts, resolver, attribute)
    } else {
      None
    }
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * Different from resolveAsTableColumn, this assumes `name` does NOT start with a qualifier.
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsColumn(
      nameParts: Seq[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    if (!attribute.isGenerated && resolver(attribute.name, nameParts.head)) {
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /** Performs attribute resolution given a name and a sequence of possible attributes. */
  protected def resolve(
      nameParts: Seq[String],
      input: Seq[Attribute],
      resolver: Resolver): Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          resolveAsTableColumn(nameParts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveAsColumn(nameParts, resolver, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it.
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        // The foldLeft adds ExtractValues for every remaining parts of the identifier,
        // and aliased it with the last part of the name.
        // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
        // Then this will add ExtractValue("c", ExtractValue("b", a)), and alias the final
        // expression as "c".
        val fieldExprs = nestedFields.foldLeft(a: Expression)((expr, fieldName) =>
          ExtractValue(expr, Literal(fieldName), resolver))
        Some(Alias(fieldExprs, nestedFields.last)())

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new AnalysisException(
          s"Reference '$name' is ambiguous, could be: $referenceNames.")
    }
  }


  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
  def refresh(): Unit = children.foreach(_.refresh())

  lazy val filterPredicates = { this.flatMap{
        case Filter(cond, child) => splitAnd(cond)
        case _ => Nil
      }
  }

  def findMapping(qSet: AttributeSet, vCols: Seq[AttributeSet])
    : Option[HashMap[Attribute, Attribute]] = {
    var projectList = new HashMap[Attribute, Attribute]
    qSet.foreach{ expr =>
      if (outputSet.contains(expr)) {
        projectList += ((expr, expr))
      }
      else {
        var containingSet = vCols.find(set => set.contains(expr))
        containingSet match {
          case Some(vSet) =>
            var alias = vSet.toSeq.find(attr => outputSet.contains(attr))
            alias match {
              case Some(aliasCol) => projectList += ((expr, aliasCol))
              case None => return None
            }
          case None => return None
        }
      }
    }
    Some(projectList)
  }

  def checkRangeBounds(view: Seq[RangeBound], query: Seq[RangeBound]): Boolean = {
    view.foreach{ vrb => query.find{ qry => vrb.cols.subsetOf(qry.cols)}.foreach{ qrb =>
       if (vrb.min > qrb.min || vrb.max < qrb.max) { return false}
      }
    }
    true
  }

  def getRangeBounds(attrSet: Seq[AttributeSet], ranges: Seq[Expression])
    : Seq[RangeBound] = {
      var rangeBounds = attrSet.map{ set =>
        RangeBound(set, Float.MinValue, Float.MaxValue, false, false)}
      ranges.foreach{
        case GreaterThan(l: Attribute, r) =>
          rangeBounds.find( rb => rb.cols.contains(l))
          .foreach{ rb => var num = r.eval(null).asInstanceOf[Float]
              if( num > rb.min) { rb.min = num ; rb.minInclusive = false }
        }
        case GreaterThanOrEqual(l: Attribute, r) =>
          var temp = rangeBounds.find( rb => rb.cols.contains(l))
          temp.foreach{ rb => var num = r.eval(null).asInstanceOf[Float]
              if( num > rb.min) { rb.min = num ; rb.minInclusive = true }
         }
         case LessThan(l: Attribute, r) =>
          var temp = rangeBounds.find( rb => rb.cols.contains(l))
          temp.foreach{ rb => var num = r.eval(null).asInstanceOf[Float]
              if( num < rb.max) { rb.min = num ; rb.maxInclusive = false }
        }
        case LessThanOrEqual(l: Attribute, r) =>
          var temp = rangeBounds.find( rb => rb.cols.contains(l))
          temp.foreach{ rb => var num = r.eval(null).asInstanceOf[Long]
              if( num > rb.max) { rb.min = num ; rb.maxInclusive = true }
         }
      }
      rangeBounds
  }

  def splitAnd(cond: Expression): Seq[Expression] = {
    cond match {
     case And(cond1, cond2) => splitAnd(cond1) ++ splitAnd(cond2)
     case otherExpr => otherExpr :: Nil
     }
   }

   def sortPredicates: (Array[Expression], Array[Expression], Array[Expression]) = {
     var equalSets = new ArrayBuffer[Expression]
     var rangeSet = new ArrayBuffer[Expression]
     var otherSet = new ArrayBuffer[Expression]
     filterPredicates.foreach{ cond =>
      cond match {
        case cond @ EqualTo(l: AttributeReference, r: AttributeReference) => equalSets += cond
        case cond @ EqualNullSafe(l: AttributeReference, r: AttributeReference) => equalSets += cond
        case cond @ GreaterThan(l: AttributeReference, r) => rangeSet += cond
        case GreaterThan(l, r: AttributeReference) => rangeSet += LessThanOrEqual(r, l)
        case cond @ GreaterThanOrEqual(l: AttributeReference, r) => rangeSet += cond
        case GreaterThanOrEqual(l, r: AttributeReference) => rangeSet += LessThan(r, l)
        case cond @ LessThan(l: AttributeReference, r) => rangeSet += cond
        case LessThan(l, r: AttributeReference) => rangeSet += GreaterThanOrEqual(r, l)
        case cond @ LessThanOrEqual(l: AttributeReference, r) => rangeSet += cond
        case LessThanOrEqual(l, r: AttributeReference) => rangeSet += GreaterThan(r, l)
        case _ => otherSet += cond.canonicalized
    }
  }
  (equalSets.toArray, rangeSet.toArray, otherSet.toArray)
}

 def getEqualSets(att: AttributeSet, equalExprs: Seq[Expression]): Array[AttributeSet] = {
    var attributes = new ArrayBuffer[AttributeSet]
    att.foreach( attr => attributes += AttributeSet(attr))
    equalExprs.foreach{
      case bo @ BinaryOperator(l: Attribute, r: Attribute) =>
          var fst = attributes.indexWhere( elem => elem.contains(l))
          var snd = attributes.indexWhere( elem => elem.contains(r))
          if(fst != snd && fst > 0 && snd > 0) {
            attributes(fst) = attributes(fst) ++ attributes(snd)
            attributes.remove(snd)
          }
        }
    attributes.toArray
  }

/* Change to look only at root node and Join operands
def contains(queryPlan: LogicalPlan): Option[LogicalPlan] = {
  var mapNodes = this :: Nil
  this.foreach{ node => node match {
    case j: Join => mapNodes = node :: mapNodes
    case _ =>
  }
  }

  mapNodes.foreach(node => node._contains(queryPlan) match {
    case Some(plan) => return Some(plan)
    case None =>
    } )
  None
}
*/

def contains(queryPlan: LogicalPlan): Boolean = {

    // Preliminary check that this view contains all the base tables of the subquery
    if (!queryPlan.collectLeaves.toSet.subsetOf(collectLeaves.toSet)) {
      return false
    }

    if (!queryPlan.outputSet.subsetOf(outputSet) || queryPlan.inputSet.subsetOf(outputSet)) {
      return false
    }

    val (qee, qre, qoe) = queryPlan.sortPredicates
    val (vee, vre, voe) = sortPredicates

    var qes = queryPlan.getEqualSets(queryPlan.inputSet, qee)
    var ves = getEqualSets(inputSet, vee)

    // residual subsumption test -- ignore equivalence classes for now
    val residualTest = voe.forall(expr => qoe.contains(expr))

    // equality subsumption test
    val equalityTest = ves.forall(viewSet => viewSet.size == 1 ||
          qes.exists(querySet => viewSet.subsetOf(querySet)))

    // range subsumption test
    var queryBounds = getRangeBounds(qes, qre)
    var viewBounds = getRangeBounds(ves, vre)

    val rangeTest = checkRangeBounds(viewBounds, queryBounds)
    if( !residualTest || !equalityTest || !rangeTest) {
      return false
    }

    return true
  }

/*
  def generateVariant(): String = {
        val (vee, vre, voe) = sortPredicates
        var projectList = queryExecution.analyzed.outputSet

        val r = scala.util.Random
        String temp = ""
        vee.foreach{ expr => if (r.nextInt(100) >= 50) {
            temp += expr.toString
          }
        }

        vre.foreach{ expr => if (r.nextInt(100) >= 50) {
            temp += expr.toString()
          }
        }

        voe.foreach{ expr => if (r.nextInt(100) >= 50) {
            temp += expr.toString()
          }
        }

        temp

  }

*/
}
/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override final def children: Seq[LogicalPlan] = child :: Nil

  /**
   * Generates an additional set of aliased constraints by replacing the original constraint
   * expressions with the corresponding alias
   */
  protected def getAliasedConstraints(projectList: Seq[NamedExpression])
  : Set[Expression] = {
    var allConstraints = child.constraints.asInstanceOf[Set[Expression]]
    projectList.foreach {
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints -- child.constraints
  }

  override protected def validConstraints: Set[Expression] = child.constraints

  override def statistics: Statistics = {
    // There should be some overhead in Row object, the size should not be zero when there is
    // no columns, this help to prevent divide-by-zero error.
    val childRowSize = child.output.map(_.dataType.defaultSize).sum + 8
    val outputRowSize = output.map(_.dataType.defaultSize).sum + 8
    // Assume there will be the number of rows as child has.
    var sizeInBytes = (child.statistics.sizeInBytes * outputRowSize) / childRowSize
    if (sizeInBytes == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      sizeInBytes = 1
    }

    child.statistics.copy(sizeInBytes = sizeInBytes)
  }
 }
/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}

case class RangeBound(var cols: AttributeSet, var min: Float, var max: Float,
 var minInclusive: Boolean, var maxInclusive: Boolean)
