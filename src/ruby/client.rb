require 'socket'
require 'msgpack'
require 'pry'
require 'mkmf' #for find_executable

class TagDB
  TOGET = 0
  TOPUT = 1
  TODEL = 2
  TODELALL = 3

  TBGET = 4
  TBPUT = 5
  TBDEL = 6

  TSAVE = 7

  def initialize(path = "./socket")
    @sock = UNIXSocket.new(path)
  end

  def self.spawn(db_path: nil, sock_path: "./socket", binary_path: find_executable('theban_db_server'))
    args = [binary_path,"-s",sock_path]
    args << [db_path] if db_path
    puts args.join(" ")
    io = IO.popen(args)
    at_exit{
      Process.kill("INT",io.pid)
      puts io.read()
      io.close()
    }
    sleep(1)
    return self.new(sock_path)
  end

  def query(struct)
    @sock.write(struct.to_msgpack)
    resp = MessagePack::Unpacker.new(@sock).read
    case resp
      when [0,[]] then return :ok
      else return resp
    end
  end

  def get_resp(query_resp)
    res = {}
    query_resp[1][0].each_pair do |(from,to), values| 
      cur_query_rng = (from..to)
      cur_query_res = {}
      values.each do |((from,to),val)|
        cur_query_res[(from..to)] = val.map(&:chr).join
      end
      res[cur_query_rng] = cur_query_res
    end
    return res
  end

  def bget_resp(query_resp)
    res = {}
    query_resp[1][0].each_pair do |(from,to), values| 
      cur_query_rng = (from..to)
      cur_query_res = {}
      values.each do |((from,to),(entry_size,val))|
        cur_query_res[(from..to)] = val.map(&:chr).join
      end
      res[cur_query_rng] = cur_query_res
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
    write.each_pair.map{|k,v| [k.min, k.max, v.bytes.to_a]}
  end

  def convert_write_arg_query(type, table, args)
    [type, [table, args.each_pair.map{|k,v| [[k.min, k.max], v.to_str.bytes.to_a]}]]
  end

  def convert_range_arg_query(type, table, args)
    [type, [table, args.map{|k,v| [k.min, k.max]}]]
  end

  def get(tbl, ranges)
    get_resp(query( convert_range_arg_query(TOGET,tbl,ranges ) ))
  end

  def del(tbl, ranges)
    query( convert_range_arg_query(TODEL,tbl, ranges) )
  end

  def delall(tbl, ranges)
    query( convert_range_arg_query(TODELALL,tbl, ranges) )
  end

  def put(tbl, writes)
    query( convert_write_arg_query(TOPUT,tbl, writes) )
  end

  def bget(tbl, ranges)
    bget_resp(query( convert_range_arg_query(TBGET,tbl, ranges) ))
  end

  def bdel(tbl, ranges)
    query( convert_range_arg_query(TBDEL, tbl, ranges) )
  end

  def bput(tbl, writes)
    query( convert_write_arg_query(TBPUT,tbl, writes) )
  end

  def saveas(file)
    query( [TSAVE, [file]])
  end

end
