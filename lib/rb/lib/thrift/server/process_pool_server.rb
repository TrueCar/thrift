#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

require 'thread'

module Thrift
  class ProcessPoolServer < BaseServer
    attr_reader :max_pool_size
    def initialize(processor, server_transport, transport_factory=nil, protocol_factory=nil, max_pool_size=10)
      super(processor, server_transport, transport_factory, protocol_factory)
      @pool_size = 0
      @max_pool_size = max_pool_size
      @exception_q = Queue.new
      @running = false
      @child_exceptions = {}
      enable_serving
    end

    ## exceptions that happen in worker threads will be relayed here and
    ## must be caught. 'retry' can be used to continue. (threads will
    ## continue to run while the exception is being handled.)
    def rescuable_serve
      Thread.new { serve } unless @running
      @running = true
      raise @exception_q.pop
    end

    ## exceptions that happen in worker process simply cause that thread
    ## to die and another to be spawned in its place.
    def serve
      @server_transport.listen

      begin
        parent_pid = Process.pid
        loop do
          break unless @serving_enabled
          io_read, io_write = IO.pipe
          pid = Process.fork do
            trap("SIGINT") do
              exit
            end

            io_read.close
            begin
              loop do
                client = @server_transport.accept
                trans = @transport_factory.get_transport(client)
                prot = @protocol_factory.get_protocol(trans)
                begin
                  loop do
                    @processor.process(prot, prot)
                  end
                rescue Thrift::TransportException, Thrift::ProtocolException => e
                rescue  => e
                  raise e
                ensure
                  trans.close
                end
              end
              Process.exit 0
            rescue => e
              io_write.puts [Marshal.dump(e)].pack("m")
              io_write.flush
              Process.exit 1
            end
          end
          io_write.close
          @child_exceptions[pid] = lambda do
            Marshal.load(io_read.read.unpack("m")[0])
          end
          if @child_exceptions.length >= @max_pool_size
            finished_pid, finished_status = Process.wait2
            if finished_status.exitstatus && finished_status.exitstatus > 0
              @exception_q.push(@child_exceptions[finished_pid].call)
            end
            @child_exceptions.delete finished_pid
          end
        end
      ensure
        @server_transport.close
      end
    end

    def enable_serving
      @serving_enabled = true
    end

    def disable_serving
      @serving_enabled = false
    end

    def kill_child_processes
      disable_serving
      @child_exceptions.keys.each do |pid|
        Process.kill("INT", pid)
      end
      enable_serving
    end
  end
end