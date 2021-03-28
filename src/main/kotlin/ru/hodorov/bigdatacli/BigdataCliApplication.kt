package ru.hodorov.bigdatacli

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class BigdataCliApplication

fun main(args: Array<String>) {
	runApplication<BigdataCliApplication>(*args)
}
