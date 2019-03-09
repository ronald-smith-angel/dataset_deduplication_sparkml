package com.sample.entities

//TODO: Add numeric correct formats in struct
case class Car(carType: Option[String],
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




