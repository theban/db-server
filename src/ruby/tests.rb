require_relative './client.rb'


db = TagDB.spawn()

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

db.saveas("test.dmp")

file = MessagePack.unpack(File.read("test.dmp"))
should = 
[
    {
      "mem" => [],
      "objs" => [ 3, 4, "blub" ]
    },
    {
        "mem" => [ 
              1, 1, [1, "a"],
              4, 6, [1, "def" ]
        ],
        "objs" => []
    }
]

assert_eq(file, should)
File.delete("test.dmp")
puts "tests successfull"
