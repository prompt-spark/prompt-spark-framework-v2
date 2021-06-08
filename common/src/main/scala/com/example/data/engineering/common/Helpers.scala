package com.example.data.engineering.common

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.runtime.{universe => runTimeUniverse}

object Helpers {

  def colMatcher(optionalCols: Set[String],
                 mainDFCols: Set[String]): List[Column] = {
    mainDFCols.toList.map {
      case x if optionalCols.contains(x) => col(x)
      case x                             => lit(null).as(x)
    }
  }

  def getCaseClassType[T: runTimeUniverse.TypeTag]
  : List[runTimeUniverse.Symbol] = {
    runTimeUniverse.typeOf[T].members.toList
  }

  def getMembers[nameCaseClass: runTimeUniverse.TypeTag]: List[String] = {
    getCaseClassType[nameCaseClass]
      .filter(!_.isMethod)
      .map(x => x.name.decodedName.toString.replaceAll(" ", ""))

  }

  def removeSpecialCharsFromCols(data: DataFrame,
                                 replace: String,
                                 replaceWith: String): DataFrame = {
    data.columns.foldLeft(data) { (renamedDf, colname) =>
      renamedDf
        .withColumnRenamed(colname, colname.replace(replace, replaceWith))
    }
  }

  def getTableNameFromCaseClass[T: runTimeUniverse.TypeTag]
  : _root_.scala.reflect.runtime.universe.TypeName =
    runTimeUniverse.symbolOf[T].name

}
