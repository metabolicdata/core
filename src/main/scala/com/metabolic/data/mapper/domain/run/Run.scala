package com.metabolic.data.mapper.domain.run

import EngineMode.EngineMode
import com.metabolic.data.mapper.domain.config.Config

import java.time.LocalDateTime
import java.util.UUID

case class Run(id: UUID, ts: LocalDateTime, engineMode: EngineMode, config: Config, confUrl: String)
