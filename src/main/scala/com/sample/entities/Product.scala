package com.sample.entities

/**
  * Case Class to handle the dataset schema.
  * All The value are String for this example.
  *
  * @param productType
  * @param city
  * @param color
  * @param contentChunk
  * @param country
  * @param date
  * @param doors
  * @param fuel
  * @param make
  * @param mileage
  * @param model
  * @param price
  * @param region
  * @param titleChunk
  * @param transmission
  * @param uniqueId
  * @param urlAnonymized
  * @param year
  */
case class Product(productType: Option[String],
                   city: Option[String],
                   color: Option[String],
                   contentChunk: Option[String],
                   country: Option[String],
                   date: Option[String],
                   doors: Option[String],
                   fuel: Option[String],
                   make: Option[String],
                   mileage: Option[String],
                   model: Option[String],
                   price: Option[String],
                   region: Option[String],
                   titleChunk: Option[String],
                   transmission: Option[String],
                   uniqueId: Option[String], // Used as Unique doc Id for elastic search
                   urlAnonymized: Option[String],
                   year: Option[String]
                  )




