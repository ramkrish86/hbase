#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    class EnableTraces < Command
      def help
        <<-EOF
Enable disable traces dynamically for given regionserver
          Examples:
          To enable traces
          hbase> compact_rs 'host187.example.com,60020,1289493121758', true
          To disable traces
          hbase> compact_rs 'host187.example.com,60020,1289493121758', false

EOF
      end

      def command(regionserver, value)
        admin.enableTraces(regionserver,value)
      end
    end
  end
end
