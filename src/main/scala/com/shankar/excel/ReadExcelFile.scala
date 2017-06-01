package com.shankar.excel

import java.io.{File, FileInputStream}

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Cell, CellType}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

/**
  * Created by sakoirala on 5/31/17.
  */
object ReadExcelFile extends App{

  val START_ROW = 6
  val END_ROW   = 493
  val COLUMN_COUNT = 31


  val myFile = new File("/home/sakoirala/Downloads/eu-historical-price-series_en.xls")

  val fis = new FileInputStream(myFile)

  val myWorkbook = new HSSFWorkbook(fis)

  val mySheet = myWorkbook.getSheetAt(0)

  val rowIterator = mySheet.iterator()

  var count = 1
  while(rowIterator.hasNext){

    val row = rowIterator.next()

    if (count > 6 && count < 493){

      val cellIterator = row.cellIterator()

      var columnCount = 1
      while(cellIterator.hasNext) {
        val cell = cellIterator.next()
        if (columnCount <= COLUMN_COUNT) {
          cell.getCellType match {
            case Cell.CELL_TYPE_STRING => {
              print(cell.getStringCellValue + "\t")
            }
            case Cell.CELL_TYPE_NUMERIC => {
              print(cell.getNumericCellValue + "\t")
            }
            case Cell.CELL_TYPE_BOOLEAN => {
              print(cell.getBooleanCellValue + "\t")
            }
            case Cell.CELL_TYPE_BLANK => {
              print("null" + "\t")
            }
            case _ => throw new RuntimeException(" this error occured when reading ")
            //        case Cell.CELL_TYPE_FORMULA => {print(cell.getF + "\t")}
          }
        }
        columnCount += 1
      }
      println("")
    }
    count += 1
  }


}
