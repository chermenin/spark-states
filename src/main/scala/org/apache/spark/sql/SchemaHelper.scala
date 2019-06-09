package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{DataType, StructType}

object SchemaHelper extends Logging {

  def schemaEvolution( rows: Iterator[InternalRow], srcSchema: StructType, tgtSchema: StructType): Iterator[InternalRow] = {
    val projection = getSchemaProjection(srcSchema, tgtSchema)
    logInfo( s"projection: $projection" )
    rows.map( row => applySchemaProjection(row, projection))
  }

  def evaluateExpr(exprCol: Column, in: InternalRow, schema: StructType): Any = {
    val attributes = schema.toAttributes.map( a => a.withName(a.name))
    val expr = exprCol.expr
    val plan = LocalRelation(attributes)
    val resolvedExpr = SimpleAnalyzer.resolveExpression(expr, plan)
    val boundExpr = BindReferences.bindReference(resolvedExpr, attributes)
    boundExpr.eval(in)
  }

  sealed trait FieldProjector {
    def get( in: InternalRow ): Any
  }

  case class CopyFieldProjector(name: String, srcIdx: Int, srcType: DataType ) extends FieldProjector {
    def get( in: InternalRow ): Any = {
      in.get(srcIdx, srcType)
    }
  }

  case class StructTypeFieldProjector(name: String, srcIdx: Int, srcSchema: StructType, tgtSchema: StructType ) extends FieldProjector {
    private val projection = getSchemaProjection(srcSchema, tgtSchema)
    def get( in: InternalRow ): Any = {
      val fieldRow = in.getStruct(srcIdx, srcSchema.size)
      applySchemaProjection(fieldRow, projection)
    }
  }

  case class NewFieldProjector(name: String, tgtType: DataType, defaultValue: Any = null ) extends FieldProjector {
    def get(in: InternalRow): Any = defaultValue
  }

  def getSchemaProjection(srcSchema: StructType, tgtSchema: StructType): Seq[FieldProjector] = {
    tgtSchema.map {
      tgtField =>
        val srcField = srcSchema.find( f => f.name == tgtField.name)
        if (srcField.isDefined) {
          // if field already exists, manage data type evolution
          (srcField.get.dataType, tgtField.dataType) match {
            case (srcType,tgtType) if srcType==tgtType => CopyFieldProjector(srcField.get.name, srcSchema.fieldIndex(srcField.get.name), srcType)
            case (srcType:StructType,tgtType:StructType) => StructTypeFieldProjector(srcField.get.name, srcSchema.fieldIndex(srcField.get.name), srcType, tgtType)
            case (srcType,tgtType) => throw new SchemaEvolutionException(s"schema evolution from $srcType to $tgtType not supported (field ${tgtField.name})")
          }
        } else {
          // if field doesn't yet exist, create an empty field
          NewFieldProjector(tgtField.name, tgtField.dataType)
        }
    }
  }

  def applySchemaProjection( in: InternalRow, projection: Seq[FieldProjector]): InternalRow = {
    InternalRow.fromSeq( projection.map( _.get(in)))
  }

  class SchemaEvolutionException(msg:String) extends Exception(msg)

}
