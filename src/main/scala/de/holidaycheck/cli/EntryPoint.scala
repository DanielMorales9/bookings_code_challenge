package de.holidaycheck.cli

import wvlet.airframe.launcher.{command, option}

class EntryPoint(
    @option(
      prefix = "-h,--help",
      description = "display help messages",
      isHelp = true
    )
    help: Boolean = false
) {

  @command(isDefault = true)
  def default(): Unit = {
    println("Type --help to display the list of commands")
  }

}
