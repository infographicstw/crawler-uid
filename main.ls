require! <[fs request bluebird]> 

console.log "[ Company Data Crawler ]"

fetch = (id) -> new bluebird (res, rej) ->
  (e,r,b) <- request {
    url: "http://gcis.nat.g0v.tw/api/show/#id"
    method: \GET
  }, _
  if e or !b => return rej "no data for #id: #e"
  try
    ret = JSON.parse(b)
  catch
    rej e
  res ret

state = do
  data: do
    idlist: null
    done: null
  todo: []
  is-done: -> (@data.done[@data.idlist.indexOf(it)] == 49)
  set:  -> @data.done[@data.idlist.indexOf(it)] = 49
  save: -> fs.write-file-sync \state.json, JSON.stringify(@data)
  init: -> 
    console.log "initializing state machine..."
    if !(fs.exists-sync(\state.json)) => 
      console.log "No state found, generate from ID list..."
      # this should be point to the uid list csv file you want to use ...
      ids = fs.read-file-sync(\uid-list/2015-04-28.csv).toString!split(\\n).map(-> 
        ret = /^(\d{8}),/.exec it
        if !ret => return null
        return ret.1
      ).filter(->it)
      console.log "sorting ID list..."
      ids.sort!
      @data.idlist = ids
      console.log "establish state hash..."
      @data.done = new Buffer("0" * ids.length)
      console.log "save state hash..."
      @save!
    else
      console.log "Loading crawler state..."
      @data = JSON.parse(fs.read-file-sync(\state.json)toString!)
    for i from 0 til @data.done.length => if @data.done[i] == 48 => @todo.push @data.idlist[i] 
    console.log "initialized."

state.init!

store = do
  root: \data
  flush-count: 100
  count: 0
  hash: null
  key: null

  set-flush-callback: -> @cb = it

  path: -> "#{@root}/#{@key}.json"
  flush: ->
    if @key and @hash => 
      fs.write-file-sync @path!, JSON.stringify(@hash)
      @cb!

  write: (id, data) ->
    store = @get(id)
    store[id] = data
    @count += 1
    if @count >= @flush-count =>
      @count = 0
      @flush!

  get: (id) ->
    if !@key or @get-key(id) != @key =>
      @flush!
      @count = 0
      @key = @get-key(id)
      if fs.exists-sync @path! => 
        @hash = JSON.parse(fs.read-file-sync(@path!)toString!)
      else => @hash = {}
    @hash

  get-key: (id) -> "#id".substring(0,4)

fetchlist = (list) ->
  while true
    if list.length == 0 => return store.flush!
    item = list.splice 0,1 .0
    if !state.is-done(item) => break
  console.log "remains #{list.length + 1} ... / retrieving #item "
  fetch item .then (v) ->
    state.set item
    store.write item, v
    fetchlist list
  .catch (e) ->
    console.log e
    console.log e.toString!
    console.log "failed at #item. retry..."
    list.push item
    setTimeout (-> fetchlist list), 1000 + Math.random!* 500

store.set-flush-callback -> state.save!

console.log "start crawling company details..."
fetchlist state.todo
