#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""agg [--groupby=N] [--sum=N] [--count=N] [--countuniq=N] [--concat=N] [--uniqconcat=N] [--max=N] [--min=N] [--file=OUTPUT_FILE] [--seperator] [inputfile]

DESCRIPTION

    Commandline tool for aggregating CSV files. 

EXAMPLES

    cat data.csv | agg --groupby=1 --count=2 --sum=2

AUTHOR

    Ole Morten Grodas <grodaas@gmail.com>

"""

import optparse
import getopt
import sys
import itertools

from collections import defaultdict


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["groupby=","sum=","count=","countuniq=","concat=","concatuniq=","max=","min=","seperator=","concat-seperator=","file=","help"])
    except getopt.GetoptError, err:
        usage()
        sys.exit(2)

    if args:
        inputstream=itertools.chain(*(open(arg) for arg in args))
    else:
        inputstream=sys.stdin
        
    aggregators=[]
    keycols=[]
    seperator="|"
    concat_seperator=","
    outputstream=sys.stdout
    for opt, arg in opts:
        if opt in ("--groupby"):
            keycols.extend(int(el) for el in arg.split(","))
        elif opt in ( "--sum"):
            aggregators.extend(Summer(el) for el in arg.split(","))
        elif opt in ( "--count"):
            aggregators.extend(Counter() for el in arg.split(","))
        elif opt in ( "--countuniq"):
            aggregators.extend(UniqCounter(el) for el in arg.split(","))
        elif opt in ( "--concat"):
            aggregators.extend(Concatter(el,concat_seperator) for el in arg.split(","))
        elif opt in ( "--concatuniq"):
            aggregators.extend(UniqConcatter(el,concat_seperator) for el in arg.split(","))
        elif opt in ( "--max"):
            aggregators.extend(Maxer(el) for el in arg.split(","))
        elif opt in ( "--min"):
            aggregators.extend(Miner(el) for el in arg.split(","))
        elif opt in ( "--seperator"):
            seperator=arg
        elif opt in ( "--concat--seperator"):
            concat_seperator=arg
        elif opt in ("--file"):
            outputstream=open(arg,"a+",0)
        elif opt in ('-h','--help'):
            usage()
            sys.exit()
        else:
            assert False, "Invalid option"
    
    db=defaultdict(dict)
    for line in inputstream:
        new_elements=line.split(seperator) 
        agg_key=tuple(new_elements[i] for i in keycols)
        agg_record=db[agg_key]
        for i,aggregator in enumerate(aggregators):
            agg_record[i]=aggregator.add(agg_record.setdefault(i),new_elements)
            
    for agg_key in db:
        line=[]
        line.extend(agg_key)
        aggregated_record=db[agg_key]
        for i,aggregator in enumerate( aggregators ):
            line.append ( str (aggregator.get( aggregated_record [i] ) ) )
        outputstream.write ( seperator.join ( line ) + '\n')
    return 0
    
def usage ():
    print globals()['__doc__']
 
def nesteddict():
    return defaultdict(nesteddict)


class Summer(object):
    def __init__(self,pos):
        self.pos=int(pos)

    def add(self,aggrec,new):
        if aggrec:
            return aggrec+int(new[self.pos])
        else:
            return int(new[self.pos])

    def get(self,aggrec):
        return aggrec

class Counter(object):
    def add(self,aggrec,new):
        if aggrec:
            return aggrec+1
        else:
            return 1

    def get(self,aggrec):
        return aggrec
    

class UniqCounter(object):
    def __init__(self,pos):
        self.pos=int(pos)
    def add(self,aggrec,new):
        if aggrec:
            aggrec.add(new[self.pos])
            return aggrec
        else:
            return set((new[self.pos],))

    def get(self,aggrec):
        return len(aggrec)

class Concatter(object):
    def __init__(self,pos,seperator):
        self.pos=int(pos)
        self.seperator=seperator
    def add(self,aggrec,new):
        if aggrec:
            aggrec.append(new[self.pos])
            return aggrec
        else:
            return [new[self.pos]]

    def get(self,aggrec):
        return self.seperator.join(aggrec)

class UniqConcatter(object):
    def __init__(self,pos,seperator):
        self.pos=int(pos)
        self.seperator=seperator
    def add(self,aggrec,new):
        if aggrec:
            aggrec.add(new[self.pos])
            return aggrec
        else:
            return set((new[self.pos],))

    def get(self,aggrec):
        return self.seperator.join(aggrec)

class Maxer(object):
    def __init__(self,pos):
        self.pos=int(pos)
    def add(self,aggrec,new):
        try:
            int(new[self.pos])
            if not aggrec or aggrec<int(new[self.pos]):
                return int(new[self.pos])
        except:
            if not aggrec or aggrec<new[self.pos]:
                return new[self.pos]
        return aggrec

    def get(self,aggrec):
        return aggrec

class Miner(object):
    def __init__(self,pos):
        self.pos=int(pos)
    def add(self,aggrec,new):
        try:
            int(new[self.pos])
            if not aggrec or aggrec>int(new[self.pos]):
                return int(new[self.pos])
        except:
            if not aggrec or aggrec>new[self.pos]:
                return new[self.pos]
        return aggrec

    def get(self,aggrec):
        return aggrec


if __name__=="__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt,e:
        sys.stderr.write("User presdd Ctrl+C. Exiting..\n")
