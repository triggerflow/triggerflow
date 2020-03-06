"""datetime utilities.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

from datetime import datetime


def seconds_since(some_date_time):
    delta = datetime.now() - some_date_time
    return delta.total_seconds()


def authenticate_request(db, request):
    if not request.authorization or 'username' not in request.authorization or 'password' not in request.authorization:
        return False

    passwd = db.get_key(database_name='auth$', document_id='users', key=request.authorization['username'])
    return passwd == request.authorization['password']
