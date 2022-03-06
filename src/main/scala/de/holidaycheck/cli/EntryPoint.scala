package de.holidaycheck.cli

import wvlet.airframe.launcher.{command, option}
case class GlobalOption(
    @option(
      prefix = "-h,--help",
      description = "display help messages",
      isHelp = true
    )
    help: Boolean = false
)

class EntryPoint(g: GlobalOption) {

  @command(isDefault = true)
  def default(): Unit = {
    println("Type --help to display the list of commands")
  }

}
