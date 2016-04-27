require 'socket'
require 'msgpack'
require 'pry'
require 'mkmf' #for find_executable

class TagDB
  TOGET = 1
  TOPUT = 2
  TODEL = 3
  TODELALL = 4

  TBGET = 5
  TBPUT = 6
  TBDEL = 7

  TSAVE = 8

  def initialize(path = "./socket")
    @sock = UNIXSocket.new(path)
  end

  def self.spawn(db_path: nil, sock_path: "./socket", binary_path: find_executable('tag_db_server'))
    args = [binary_path,"-s",sock_path]
    args << [db_path] if db_path
    puts args.join(" ")
    io = IO.popen(args)
    at_exit{
      Process.kill("INT",io.pid)
      io.close()
    }
    sleep(1)
    return self.new(sock_path)
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

  def saveas(file)
    query( [TSAVE, file])
  end

end
