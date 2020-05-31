package org.apache.spark.sql.execution.command.mutation.merge

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object MergeColumn {
  def apply(expr: Expression): MergeColumn = new MergeColumn(expr)

  def apply(column: Column): MergeColumn = new MergeColumn(column.expr)
}

class MergeColumn(override val expr: Expression) extends Column(expr) {
  override def equals(that: Any): Boolean = {
    if(that !=null && that.isInstanceOf[Column]) {
      val col = that.asInstanceOf[Column]
      col.expr.semanticEquals(expr)
    } else {
      false
    }
  }

  override def hashCode(): Int = {
    expr.semanticHash()
  }
}
