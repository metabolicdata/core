package com.metabolic.data.mapper.domain.config

import com.metabolic.data.core.domain.Environment


case class Metadata(name: String,
                    description:String,
                    owner: String,
                    sqlUrl: String,
                    confUrl: String,
                    environment: Environment) {



  def getCanonicalName() = {
    name
      .toLowerCase()
      .replaceAll(" ", "-")
  }

}
