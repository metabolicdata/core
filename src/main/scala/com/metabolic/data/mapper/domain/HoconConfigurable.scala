package com.metabolic.data.mapper.domain

import com.typesafe.config.{Config => HoconConfig}

trait HoconConfigurable[T <: HoconConfigurable[T]] {

  def fromHocon(config: HoconConfig): Option[T]

}
