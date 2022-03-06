package de.holidaycheck

import de.holidaycheck.cli.{EntryPoint, JobEntryPoint, ReportEntryPoint}
import wvlet.airframe.launcher.Launcher

object Main extends App {
  val l = Launcher
    .of[EntryPoint]
    .addModule[JobEntryPoint](name = "job", description = "List of all Jobs")
    .addModule[ReportEntryPoint](
      name = "report",
      description = "List of all Reports"
    )

  l.execute(args)
}
