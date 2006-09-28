package provide luciddb [lindex {$Revision: 1.0 $} 1]

package require hashes

namespace eval luciddb {
    variable statsfilename "LucidDbPerfCounters.txt"

    array set data {
        updates 0
        0,label counter 0,type ascii 0,message {Counter}
        1,label value 1,type integer 1,message {Value}
        sort {0 increasing}
        indexColumns {0}
        switches { --file 1 }
    }
    set file [open luciddb.html]
    set data(helpText) [read $file]
    close $file

    proc initialize {optionsName} {
        upvar $optionsName options
        variable data
        variable statsfilename

        catch {set statsfilename $options(--file)}
        set data(pollTimes) {1 20 5 10 30 60 120 300 600}
    }

    proc update {} {
        variable data
        variable statsfilename
        
        set file [open $statsfilename]
        while {[gets $file line]>=0} {
            foreach {counter value} $line {}
            set row [updateEntryData $counter $value]
            set current($row) {}
        }
        close $file
        incr data(updates)
    }

    proc updateEntryData {counter value} {
        variable data

        set row [hash64::string $counter]

        if {![info exists data($row,0)]} {
            set data($row,0) $counter
        }
        set data($row,1) $value
        return $row
    }
}
