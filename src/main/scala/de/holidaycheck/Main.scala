package de.holidaycheck

import de.holidaycheck.cli.EntryPoint
import wvlet.airframe.launcher.Launcher

object Main extends App {
  Launcher.execute[EntryPoint](args)
}
