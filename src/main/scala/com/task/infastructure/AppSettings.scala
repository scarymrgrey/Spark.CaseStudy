package com.task.infastructure

case class AppSettings(eventsFilePath: String,
                       purchasesFilePath: String,
                       topCompaniesToShow: Int,
                       outputPurchasesPath: String,
                       outputTopCompaniesPath: String,
                       outputSessionsPath: String,
                       outputChannelEngagementsPath: String
                      )
