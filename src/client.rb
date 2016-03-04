require 'socket'
require 'msgpack'
require 'pry'
class TagDB
  TGET = 1
  TPUT = 2
  TDEL = 3
  TDELALL = 4

  def initialize(path = "socket")
    @sock = UNIXSocket.new(path)
  end

  def query(struct)
    @sock.write(struct.to_msgpack)
    resp = MessagePack::Unpacker.new(@sock).read
    return resp
  end

  def convert_ranges(ranges)
    ranges.map{|r| [r.min, r.max]}
  end

  def convert_writes(write)
    write.each_pair.map{|k,v| [k.min, k.max, v]}
  end

  def get(tbl, ranges)
    query( [TGET,tbl, convert_ranges(ranges)] )
  end

  def del(tbl, ranges)
    query( [TDEL,tbl, convert_ranges(ranges)] )
  end

  def delall(tbl, ranges)
    query( [TDELALL,tbl, convert_ranges(ranges)] )
  end

  def put(tbl, writes)
    query( [TPUT,tbl, convert_writes(writes)] )
  end

end

db = TagDB.new
puts db.put("mem", {1..5 => "foo", 4..6 => "bar"} ).inspect
puts db.get("mem",[0..100]).inspect
puts db.get("mem",[6..100]).inspect

