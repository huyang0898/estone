package com.estone.amazon.sellerSkuStats.util

trait PublisherService {
/**
 * @param date
 * @param keyword
 *
 * */
  def getSaleDetailAndAggResultByAggField(date: String,
                                          keyword: String,
                                          startPage: Int,
                                          size: Int,
                                          aggField: String,
                                          aggSize: Int): Map[String, Any]

}
