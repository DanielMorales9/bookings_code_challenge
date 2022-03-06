package de.holidaycheck.cli

import wvlet.airframe.launcher.{command, option}

class ReportEntryPoint(
    @option(
      prefix = "-h,--help",
      description = "display help messages",
      isHelp = true
    )
    help: Boolean = false
) {

  @command(description = "does nothing")
  def dummy(): Unit = {
    println("splash spalsh! Hat keine wirkung")
  }

}
