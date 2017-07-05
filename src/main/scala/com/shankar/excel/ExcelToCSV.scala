package com.shankar.excel

import java.io.{File, FileInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.Cell

/**
  * Created by sakoirala on 6/13/17.
  */
object ExcelToCSV {

  val START_ROW = 6
  val END_ROW = 493
  val COLUMN_COUNT = 31

  def excelToCSV(
      location: String,
      numberOfColumns: Int,
      numberOfRows: Int,
      sheetIndex: Int,
      csvFileName: String
  ): Unit = {

    val conf = new Configuration()
    val path = new Path(location)

    val fs = path.getFileSystem(conf)
    val fileInput = fs.open(path)


//    val fileOutput = fs.create(new path.getName +  csvFileName)


//    val myFile = new File(location)
//
//    val fis = new FileInputStream(myFile)

    val myWorkbook = new HSSFWorkbook(fileInput)

    val mySheet = myWorkbook.getSheetAt(sheetIndex)

    val rowIterator = mySheet.iterator()

    var count = 1
    while (rowIterator.hasNext) {

      val row = rowIterator.next()

      if (count > 6 && count < 492) {

        val cellIterator = row.cellIterator()

        var columnCount = 1
        while (cellIterator.hasNext) {
          val cell = cellIterator.next()
          if (columnCount <= COLUMN_COUNT) {
            val data = cell.getCellType match {
              case Cell.CELL_TYPE_STRING => {
                cell.getStringCellValue
              }
              case Cell.CELL_TYPE_NUMERIC => {
                cell.getNumericCellValue.toString
              }
              case Cell.CELL_TYPE_BOOLEAN => {
                cell.getBooleanCellValue.toString
              }
              case Cell.CELL_TYPE_BLANK => {
                null
              }
              case _ =>
                throw new RuntimeException(" this error occured when reading ")
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

}
