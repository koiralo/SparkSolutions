package com.shankar.domain

import org.spark_project.jetty.util.ArrayTernaryTrie

/**
  * Created by sakoirala on 5/10/17.
  */
case class Company(
                  id: String,
                  tradeStyleNames: Array[TradeStyleNames],
                  telephone: Array[Telephone],
                  primaryAddress: PrimaryAddress
                  ) {}

case class TradeStyleNames(name: String, priority: Int){}
case class Telephone(telephoneNumber: String, isdCode: String, isUnreachable: Boolean){}
case class PrimaryAddress(line1: String, line2: String){}