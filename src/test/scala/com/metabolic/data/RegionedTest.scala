package com.metabolic.data

import com.amazonaws.regions.Regions

trait RegionedTest {
  implicit val region = Regions.EU_CENTRAL_1
}
