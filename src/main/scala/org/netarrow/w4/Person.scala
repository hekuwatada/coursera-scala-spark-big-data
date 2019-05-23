package org.netarrow.w4

case class Person(id: Int, name: String, city: String, country: String)

case class SlimPerson(name: String)

case class Address(doorNumber: Int, city: String, country: String)
case class RichPerson(name: String, address: Address, salary: Double)
case class WorkingPerson(name: String, salary: Double)