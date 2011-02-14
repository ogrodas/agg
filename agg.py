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

import getopt
import sys
import itertools


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["groupby=","sortedgroupby=","sum=","count=","countuniq=","concat=","concatuniq=","max=","min=","seperator=","concat-seperator=","file=","help"])
    except getopt.GetoptError, err:
        usage()
        sys.exit(2)

    if args:
        inputstream=itertools.chain(*(open(arg) for arg in args))
    else:
        inputstream=sys.stdin
        
    aggregators=[]
    keycols=[]
    sorted_keycols=[]
    seperator="|"
    concat_seperator=","
    outputstream=sys.stdout
    for opt, arg in opts:
        if opt in ("--groupby"):
            keycols.extend(int(el) for el in arg.split(","))
        elif opt in ("--sortedgroupby"):
            sorted_keycols=[int(col) for col in arg.split(",")]
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
    
    def printdb(db,sorted_key):
        for agg_key in db:
            line=sorted_key[:]
            line.extend(agg_key)
            aggregated_record=db[agg_key]
            for i,aggregator in enumerate( aggregators ):
                line.append ( str (aggregator.get( aggregated_record [i] ) ) )
            outputstream.write ( seperator.join ( line ) + '\n')
        return 0
 


    db=OrderedDict()
    sorted_key=[]
    for line in inputstream:
        new_elements=line.split(seperator) 
        if sorted_keycols:
            new_sorted_key=[new_elements[col] for col in sorted_keycols]   
            if sorted_key!=new_sorted_key:
                if db: 
                    printdb(db,sorted_key)
                    db=OrderedDict()
                sorted_key=new_sorted_key
        agg_key=tuple(new_elements[i] for i in keycols)
        agg_record=db.setdefault(agg_key,{})
        for i,aggregator in enumerate(aggregators):
            agg_record[i]=aggregator.add(agg_record.setdefault(i),new_elements)
    printdb(db,sorted_key)
            
   
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


## {{{ http://code.activestate.com/recipes/576693/ (r6)
from UserDict import DictMixin

class OrderedDict(dict, DictMixin):

    def __init__(self, *args, **kwds):
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        try:
            self.__end
        except AttributeError:
            self.clear()
        self.update(*args, **kwds)

    def clear(self):
        self.__end = end = []
        end += [None, end, end]         # sentinel node for doubly linked list
        self.__map = {}                 # key --> [key, prev, next]
        dict.clear(self)

    def __setitem__(self, key, value):
        if key not in self:
            end = self.__end
            curr = end[1]
            curr[2] = end[1] = self.__map[key] = [key, curr, end]
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        key, prev, next = self.__map.pop(key)
        prev[2] = next
        next[1] = prev

    def __iter__(self):
        end = self.__end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.__end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def popitem(self, last=True):
        if not self:
            raise KeyError('dictionary is empty')
        if last:
            key = reversed(self).next()
        else:
            key = iter(self).next()
        value = self.pop(key)
        return key, value

    def __reduce__(self):
        items = [[k, self[k]] for k in self]
        tmp = self.__map, self.__end
        del self.__map, self.__end
        inst_dict = vars(self).copy()
        self.__map, self.__end = tmp
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def keys(self):
        return list(self)

    setdefault = DictMixin.setdefault
    update = DictMixin.update
    pop = DictMixin.pop
    values = DictMixin.values
    items = DictMixin.items
    iterkeys = DictMixin.iterkeys
    itervalues = DictMixin.itervalues
    iteritems = DictMixin.iteritems

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, self.items())

    def copy(self):
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        if isinstance(other, OrderedDict):
            return len(self)==len(other) and self.items() == other.items()
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self == other
## end of http://code.activestate.com/recipes/576693/ }}}


if __name__=="__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt,e:
        sys.stderr.write("User presdd Ctrl+C. Exiting..\n")
