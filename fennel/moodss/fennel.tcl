package provide fennel [lindex {$Revision: 1.0 $} 1]

namespace eval fennel {
    variable statsfilename "/tmp/fennel.stats"

    array set data {
        updates 0
        0,label counter 0,type ascii 0,message {Counter}
        1,label value 1,type integer 1,message {Value}
        sort {0 increasing}
        indexColumns {0}
        switches { --file 1 }
    }
    set file [open fennel.htm]
    set data(helpText) [read $file]
    close $file

    proc initialize {optionsName} {
        upvar $optionsName options
        variable data
        variable statsfilename

        catch {set statsfilename $options(--file)}
        set data(pollTimes) {1 20 5 10 30 60 120 300 600}
    }

    set nextIndex 0

    proc update {} {
        variable data
        variable statsfilename
        variable nextIndex
        
        set row 0
        while {$row < $nextIndex} {
            set data($row,1) 0
            incr row
        }
        set file [open $statsfilename]
        while {[gets $file line]>=0} {
            foreach {counter value} $line {}
            updateEntryData $counter $value
        }
        close $file
        incr data(updates)
    }

    proc updateEntryData {counter value} {
        variable index
        variable data
        variable nextIndex

        if {[catch {set index($counter)} row]} {                                                                      ;# new entry
            set row [set index($counter) $nextIndex]
            incr nextIndex
            set data($row,0) $counter                                                                    ;# initialize static data
        }
        set data($row,1) $value
    }
}
