package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{DataType, StructType}

object SchemaHelper extends Logging {

  def schemaEvolution( rows: Iterator[InternalRow], srcSchema: StructType, tgtSchema: StructType
                     , defaultValueCols: Map[String,Column] = Map()): Iterator[InternalRow] = {
    val projection = getSchemaProjection(srcSchema, tgtSchema, defaultValueCols )
    logInfo( s"projection: $projection" )
    rows.map( row => applySchemaProjection(row, projection))
  }

  def prepareExpr(exprCol: Column, schema: StructType): Expression = {
    val attributes = schema.toAttributes.map( a => a.withName(a.name))
    val expr = exprCol.expr
    val plan = LocalRelation(attributes)
    val resolvedExpr = SimpleAnalyzer.resolveExpression(expr, plan)
    BindReferences.bindReference(resolvedExpr, attributes)
  }

  sealed trait FieldProjector {
    def get(inRow: InternalRow, topRow: InternalRow ): Any
  }

  case class CopyFieldProjector(name: String, srcIdx: Int, srcType: DataType ) extends FieldProjector {
    def get(inRow: InternalRow, topRow: InternalRow ): Any = {
      inRow.get(srcIdx, srcType)
    }
  }

  case class StructTypeFieldProjector(name: String, srcIdx: Int, srcSchema: StructType, tgtSchema: StructType, defaultValues: Map[String,Expression], path: Seq[String] ) extends FieldProjector {
    private val projection = getSchemaProjectionInternal(srcSchema, tgtSchema, defaultValues, path)
    def get(inRow: InternalRow, topRow: InternalRow ): Any = {
      val fieldRow = inRow.getStruct(srcIdx, srcSchema.size)
      applySchemaProjection(fieldRow, topRow, projection)
    }
  }

  case class NewFieldProjector(name: String, tgtType: DataType, defaultValueExpr: Option[Expression] ) extends FieldProjector {
    defaultValueExpr.foreach( d => assert(d.dataType==tgtType, s"Data type ${d.dataType} of default value expression doesnt match with target field data type $tgtType"))
    def get(in: InternalRow, topRow: InternalRow): Any = defaultValueExpr.map(_.eval(topRow)).orNull
  }

  private def getSchemaProjectionInternal(srcSchema: StructType, tgtSchema: StructType, defaultValueExprs: Map[String,Expression], path: Seq[String] = Seq()): Seq[FieldProjector] = {
    tgtSchema.map {
      tgtField =>
        val srcField = srcSchema.find( f => f.name == tgtField.name)
        val newPath = path :+ tgtField.name
        if (srcField.isDefined) {
          // if field already exists, manage data type evolution
          (srcField.get.dataType, tgtField.dataType) match {
            case (srcType,tgtType) if srcType==tgtType =>
              CopyFieldProjector(srcField.get.name, srcSchema.fieldIndex(srcField.get.name), srcType)
            case (srcType:StructType,tgtType:StructType) =>
              StructTypeFieldProjector(srcField.get.name, srcSchema.fieldIndex(srcField.get.name), srcType, tgtType, defaultValueExprs, newPath)
            case (srcType,tgtType) =>
              throw new SchemaEvolutionException(s"schema evolution from $srcType to $tgtType not supported (field ${tgtField.name})")
          }
        } else {
          // if field doesn't yet exist, create a field and populate with default value if defined
          val defaultValue = defaultValueExprs.get(newPath.mkString("."))
          logDebug(s"using defaultValue $defaultValue for new field ${tgtField.name}")
          NewFieldProjector(tgtField.name, tgtField.dataType, defaultValue)
        }
    }
  }

  def getSchemaProjection(srcSchema: StructType, tgtSchema: StructType, defaultValueCols: Map[String,Column], path: Seq[String] = Seq()): Seq[FieldProjector] = {
    val defaultValueExprs = defaultValueCols.mapValues{ case col => prepareExpr(col, srcSchema)}
    getSchemaProjectionInternal(srcSchema, tgtSchema, defaultValueExprs, path)
  }

  def applySchemaProjection(inRow: InternalRow, topRow: InternalRow, projection: Seq[FieldProjector]): InternalRow = {
    InternalRow.fromSeq( projection.map( _.get(inRow, topRow)))
  }

  def applySchemaProjection(inRow: InternalRow, projection: Seq[FieldProjector]): InternalRow = {
    InternalRow.fromSeq( projection.map( _.get(inRow, inRow)))
  }

  class SchemaEvolutionException(msg:String) extends Exception(msg)

}
