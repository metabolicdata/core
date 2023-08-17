package com.metabolic.data.core.services.catalogue

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryExpression, Concat, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BinaryNode, GlobalLimit, LocalLimit, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable

class ColumnLineage {
  def getRootColumns(expr: Expression): mutable.Set[String] = {
    val inputColumns = mutable.Set[String]()

    expr match {
      case attr: AttributeReference =>
        inputColumns += attr.toString()

      case alias: Alias =>
        inputColumns += alias.toString()
        inputColumns ++= getRootColumns(alias.child)

      case concat: Concat =>
        concat.children.map(getRootColumns).foreach(e => inputColumns ++= e)

      case aggregate: AggregateExpression =>
        aggregate.children.map(getRootColumns).foreach(e => inputColumns ++= e)

      case avg: Average =>
        inputColumns ++= getRootColumns(avg.child)

      case max: Max =>
        inputColumns ++= getRootColumns(max.child)

      case min: Min =>
        inputColumns ++= getRootColumns(min.child)

      case unaryExpr: UnaryExpression =>
        inputColumns ++= getRootColumns(unaryExpr.child)

      case binaryExpr: BinaryExpression =>
        inputColumns ++= getRootColumns(binaryExpr.left)
        inputColumns ++= getRootColumns(binaryExpr.right)

      case _ => println(expr.getClass)
    }

    inputColumns
  }

  def traversePlan(plan: LogicalPlan): mutable.Map[String, mutable.Set[String]] = {
    val columnLineage = mutable.Map[String, mutable.Set[String]]()

    plan match {
      case GlobalLimit(limitExpr, child) =>
        columnLineage ++= traversePlan(child)

      case LocalLimit(limitExpr, child) =>
        columnLineage ++= traversePlan(child)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        aggregateExpressions.map(getInputColumns).foreach(e => columnLineage ++= e)
        columnLineage ++= traversePlan(child)

      case Project(projectList, child) =>
        projectList.map(getInputColumns).foreach(e => columnLineage ++= e)
        columnLineage ++= traversePlan(child)

      case unaryNode: UnaryNode =>
        columnLineage ++= traversePlan(unaryNode.child)

      case binaryNode: BinaryNode =>
        columnLineage ++= traversePlan(binaryNode.left)
        columnLineage ++= traversePlan(binaryNode.right)

//      case logicalRelation: LogicalRelation =>
//        val inputColumns = logicalRelation.output.map(_.toString()).to[mutable.Set]
//        columnLineage ++= inputColumns.map(c => (c, mutable.Set("SOURCE")))

      case e => println(e)
    }

    columnLineage
  }

  def getInputColumns(expr: Expression): mutable.Map[String, mutable.Set[String]] = {
    val inputColumns = mutable.Set[String]()
    val columnLineage = mutable.Map[String, mutable.Set[String]]()

    expr match {
//      case attr: AttributeReference =>
//        columnLineage += attr.toString() -> mutable.Set("SOURCE")

      case alias: Alias =>
        inputColumns ++= getRootColumns(alias.child)
        columnLineage += alias.name + "#" + alias.exprId.id -> inputColumns

      case concat: Concat =>
        concat.children.map(getRootColumns).foreach(e => inputColumns ++= e)
        columnLineage += concat.toString() -> inputColumns

      case aggregate: AggregateExpression =>
        aggregate.children.map(getRootColumns).foreach(e => inputColumns ++= e)
        columnLineage += aggregate.toString() -> inputColumns

      case avg: Average =>
        inputColumns ++= getRootColumns(avg.child)
        columnLineage += avg.toString() -> inputColumns

      case max: Max =>
        inputColumns ++= getRootColumns(max.child)
        columnLineage += max.toString() -> inputColumns

      case min: Min =>
        inputColumns ++= getRootColumns(min.child)
        columnLineage += min.toString() -> inputColumns

      case unaryExpr: UnaryExpression =>
        inputColumns ++= getRootColumns(unaryExpr.child)
        columnLineage += unaryExpr.toString() -> inputColumns

      case binaryExpr: BinaryExpression =>
        inputColumns ++= getRootColumns(binaryExpr.left)
        inputColumns ++= getRootColumns(binaryExpr.right)
        columnLineage += binaryExpr.toString() -> inputColumns

      case _ => println(expr.getClass)
    }

    columnLineage
  }
}
