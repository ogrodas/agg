#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""agg [--groupby=N] [--sum=N] [--count=N] [--countuniq=N] [--concat=N] [--uniqconcat=N] [--max=N] [--min=N] [--file=OUTPUT_FILE] [--seperator] [inputfile]

DESCRIPTION

    Commandline tool for aggregating CSV files. 

EXAMPLES

    cat data.csv | agg --groupby=1 --count --sum=2

AUTHOR

    Ole Morten Grodas <grodaas@gmail.com>

"""

import getopt
import sys
import itertools


def main(argv):
    try:
        
        opts, args = getopt.getopt(argv, "h", ["groupby=","count","sum=","countuniq=","concat=","concatuniq=","max=","min=","first=","last=","seperator=","concat-seperator=","colorder=","inputsorted=","file=","strip","help"])
    except getopt.GetoptError, err:
        print str(err)
        print "agg --help for more information"
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
    col_strip=False
    colorder=[]
    inputsorted=[]
    for opt, arg in opts:
        if opt in ("--groupby","--sum","--countuniq","concatuniq","--max","--min","--last","--fist","--colorder","--inputsorted"):
            try:
                columns=[int(col) for col in arg.split(",")]
            except ValueError,e:
                sys.stderr.write("Error in arguments to option %s. Could not parse: '%s'\n" % (opt,arg))
                print (e)
                sys.exit(2)
        if opt in ("--groupby="):
            keycols.extend(int(el) for el in columns)
        elif opt in ( "--sum="):
            aggregators.extend(Summer(el) for el in columns)
        elif opt in ( "--count="):
            aggregators.append(Counter())
        elif opt in("--countuniq="):
            aggregators.extend(UniqCounter(el) for el in columns)
        elif opt in ( "--concat="):
            aggregators.extend(Concatter(el,concat_seperator) for el in columns)
        elif opt in ( "--concatuniq="):
            aggregators.extend(UniqConcatter(el,concat_seperator) for el in columns)
        elif opt in ( "--max="):
            aggregators.extend(Maxer(el) for el in columns)
        elif opt in ( "--min="):
            aggregators.extend(Miner(el) for el in columns)
        elif opt in ( "--last="):
            aggregators.extend(Last(el) for el in columns)
        elif opt in ( "--first="):
            aggregators.extend(First(el) for el in columns)
        elif opt in ( "--colorder="):
            colorder=[int(col) for col in columns]
        elif opt in ( "--seperator"):
            seperator=arg
        elif opt in ( "--concat-seperator"):
            concat_seperator=arg
        elif opt in ("--file"):
            outputstream=open(arg,"a+",0)
        elif opt in ("--strip"):
            col_strip=True
        elif opt in ("--inputsorted"):
            inputsorted=columns
        elif opt in ('-h','--help'):
            usage()
            sys.exit()
        else:
            sys.stderr.write("Invalid option: %s %s\n" % (opt,arg))
            sys.exit(2)

    numcols=len(keycols) + len(aggregators)
    if colorder:
        if max(colorder) > len(aggregators):
            sys.stderr.write("Error in colorder argument: There is fewer than %i columns in the output\n" % max(colorder))
            sys.exit(2)
        if len(set(colorder))!=len(colorder):
            sys.stderr.write("Warning: Duplicates in colroder argument\n")

    for i in range(len(keycols) +len(aggregators)):
        if i not in colorder:
            colorder.append(i)

    sorted_keycols=get_sorted_keycols(keycols,inputsorted)
    
    def printdb(db):
        for agg_key in db:
            line=[]
            line.extend(agg_key)
            aggregated_record=db[agg_key]
            for i,aggregator in enumerate( aggregators ):
                line.append ( str (aggregator.get( aggregated_record [i] ) ) )  
            outputline=(line[i] for i in colorder)
            outputstream.write ( seperator.join ( outputline ) + '\n')
        return 0
 
    db=dict()
    old_sortedkey=[]
    new_sortedkey=[]
    for line in inputstream:
        line=line.rstrip( "\n" )
        if not line: continue
        if col_strip:
            new_elements=[col.strip() for col in line.split(seperator)]
        else:
            new_elements=line.split(seperator)
        if sorted_keycols:
            new_sortedkey=[new_elements[i] for i in sorted_keycols]
            if new_sortedkey!=old_sortedkey:
                if db:
                    printdb(db)
                    db={}
        agg_key=tuple(new_elements[i] for i in keycols)
        agg_record=db.setdefault(agg_key,{})
        for i,aggregator in enumerate(aggregators):
            agg_record[i]=aggregator.add(agg_record.setdefault(i),new_elements)
        old_sortedkey=new_sortedkey
    printdb(db)
            
   
def usage ():
    print globals()['__doc__']
 

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


class First(object):
    def __init__(self,pos):
        self.pos=int(pos)
    def add(self,aggrec,new):
        if not aggrec:
            return new[self.pos]
        else:
            return aggrec
    def get(self,aggrec):
        return aggrec

class Last(object):
    def __init__(self,pos):
        self.pos=int(pos)
    def add(self,aggrec,new):
        return new[self.pos]
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


def get_sorted_keycols(aggkey, sortkey):
    """if the order of the input data is unkown nothing can be printed before all of 
the inputdata is processed. If one or more of the columns in the aggregation key is 
sorted in the input data we know that when ever the value of these columns 
changes it will never change back. Because of this the the aggregation so far
can be printed and flushed from memory. This will reduce the memory recqueriments 
on large datasets."""
    for i in range (len(sortkey),0,-1):
        if sublistExists(aggkey,sortkey[0:i]):
            print "sortkey",sortkey[0:i]
            return sortkey[0:i]
    return []

def sublistExists(mainlist, sublist):
    print mainlist
    print sublist
    for i in range(len(mainlist)-len(sublist)+1):
        if sublist == mainlist[i:i+len(sublist)]:
            return True #return position (i) if you wish
    return False #or -1


if __name__=="__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt,e:
        sys.stderr.write("User presdd Ctrl+C. Exiting..\n")
    except IOError,e:
        print str(e)
        if e.errno==32: #Broken Pipe, happend typically when piping to the commandline tool head
            pass
        else:
            raise
