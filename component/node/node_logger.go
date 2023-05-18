package node

import "log"

var logPrintln = func(v ...any) {
	log.Println(v)
}

var logPrintf = func(format string, v ...any) {
	log.Printf(format, v)
}

var logPrint = func(v ...any) {
	log.Print(v)
}

var logFatalln = func(v ...any) {
	log.Fatalln(v)
}
