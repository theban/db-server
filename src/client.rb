require 'socket'
require 'msgpack'
require 'pry'
class TagDB
  TOGET = 1
  TOPUT = 2
  TODEL = 3
  TODELALL = 4

  TBGET = 5
  TBPUT = 6
  TBDEL = 7

  def initialize(path = "socket")
    @sock = UNIXSocket.new(path)
  end

  def query(struct)
    @sock.write(struct.to_msgpack)
    resp = MessagePack::Unpacker.new(@sock).read
    case resp
      when [1] then return :ok
      else return resp
    end
  end

  def get_resp(query_resp)
    res = {}
    query_resp.each do |(from,to,val)|
      res[(from..to)] = val
    end
    return res
  end

  def bget_resp(query_resp)
    res = {}
    query_resp.each do |(from,to,entry_size,val)|
      res[(from..to)] = [entry_size,val]
    end
    return res
  end

  def assert(val)
    raise "assertion failed" unless val
  end

  def convert_ranges(ranges)
    ranges.map{|r| [r.min, r.max]}
  end

  def convert_writes(write)
    write.each_pair{|k,v| assert(v.is_a? String)}
    write.each_pair.map{|k,v| [k.min, k.max, v]}
  end

  def get(tbl, ranges)
    get_resp(query( [TOGET,tbl, convert_ranges(ranges)] ))
  end

  def del(tbl, ranges)
    query( [TODEL,tbl, convert_ranges(ranges)] )
  end

  def delall(tbl, ranges)
    query( [TODELALL,tbl, convert_ranges(ranges)] )
  end

  def put(tbl, writes)
    query( [TOPUT,tbl, convert_writes(writes)] )
  end

  def bget(tbl, ranges)
    bget_resp(query( [TBGET,tbl, convert_ranges(ranges)] ))
  end

  def bdel(tbl, ranges)
    query( [TBDEL,tbl, convert_ranges(ranges)] )
  end

  def bput(tbl, writes)
    query( [TBPUT,tbl, convert_writes(writes)] )
  end

end


db = TagDB.new

def assert_eq(a,b)
  binding.pry unless a == b
end

assert_eq( db.put( "objs", {1..5 => "foo", 4..6 => "bar", 3..4 => "blub", 100..1001 => "fnord"} ),      :ok )
assert_eq( db.put( "objs", {1..5 => "bla"} ),                                                           :ok )

assert_eq( db.get( "objs",[0..99] ),                               {4..6=>"bar", 3..4=>"blub", 1..5=>"bla"} )
assert_eq( db.get( "objs",[6..100] ),                                     {4..6=>"bar", 100..1001=>"fnord"} )

assert_eq( db.del( "objs", [1..5] ),                                                                    :ok )
assert_eq( db.get( "objs", [0..99] ),                                           {4..6=>"bar", 3..4=>"blub"} )

assert_eq( db.delall( "objs", [6..100] ),                                                               :ok )
assert_eq( db.get( "objs", [0..1000] ),                                                      {3..4=>"blub"} )
assert_eq( db.get( "null", [0..1000] ),                                                                  {} )

assert_eq( db.bdel( "mem", [0..1001] ),                                                                 :ok )
assert_eq( db.bput( "mem", {1..1 => "a", 2..3 => "bc", 4..6 => "def"} ),                                :ok )
assert_eq( db.bget( "mem", [0..1000] ),                                                {1..6=>[1,"abcdef"]} )
assert_eq( db.bdel( "mem", [2..3] ),                                                                    :ok )
assert_eq( db.bget( "mem", [0..1000] ),                                  {1..1=>[1,"a"], 4..6 => [1,"def"]} )
assert_eq( db.bget( "non", [0..1000] ),                                                                  {} )

puts "tests successfull"
