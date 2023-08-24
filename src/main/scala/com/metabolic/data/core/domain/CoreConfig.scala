package com.metabolic.data.core.domain

import com.metabolic.data.mapper.domain.io.EngineMode
import com.typesafe.config.ConfigFactory

abstract class CoreConfig(val defaults: Defaults = Defaults(ConfigFactory.load()),
                          val environment: Environment = Environment("", EngineMode.Batch, "", false, "","", Option.empty, Option.empty))