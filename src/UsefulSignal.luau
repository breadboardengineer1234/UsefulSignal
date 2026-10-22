--!optimize 2
--!strict

export type Connection<T...> = {
	Connected: boolean,
	Disconnect: (self: Connection<T...>) -> (),
}
export type Signal<T...> = {
	new: () -> Signal<T...>,
	Fire: (self: Signal<T...>, T...) -> (),
	Wait: (self: Signal<T...>) -> T...,
	Once: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	Connect: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	ConnectSync: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	DisconnectAll: (self: Signal<T...>) -> (),
}
type Node<T...> = Connection<T...> & {
	_isSync: boolean,
	_next: Node<T...> | false,
	_prev: Node<T...> | false,
	_callback: (T...) -> (),
	_signal: SignalInternal<T...>,
}
type SignalInternal<T...> = Signal<T...> & {
	_head: Node<T...> | false,
}

const VERSION = "v0.2.0"

local threads: {thread} = {}

local Signal = {}
local Connection = {}
Signal.__index = Signal
Connection.__index = Connection

local function noop() end

local function runCallback(callback, thread, ...)
	callback(...)
	table.insert(threads, thread)
end

local function yieldThread()
	while true do
		runCallback(coroutine.yield())
	end
end

local function connect<T...>(self: Signal<T...>, callback: (T...) -> ()): Connection<T...>
	local head = (self :: SignalInternal<T...>)._head
	local node = {
		Connected = true, 
		_isSync = false,
		_callback = callback,
		_signal = self,
		_next = false,
		_prev = false,
	} :: Node<T...>
	
	if head then
		head._prev = node
		node._next = head
	end
	(self :: SignalInternal<T...>)._head = node
	
	return (setmetatable(node, Connection) :: any) :: Connection<T...>
end

local function connectSync<T...>(self: Signal<T...>, callback: (T...) -> ()): Connection<T...>
	local conn = connect(self, callback);
	(conn :: Node<T...>)._isSync = true
	return conn
end

local function disconnect<T...>(self: Connection<T...>)
	if not self.Connected then return end
	self.Connected = false	
	self = self :: Node<T...>
	self._callback = noop
	
	if not self._prev then --head
		local nextNode = self._next
		if nextNode then
			nextNode._prev = false
		end
		self._signal._head = nextNode
	else
		local prev = self._prev
		local nextNode = self._next
		prev._next = nextNode
		if nextNode then
			nextNode._prev = prev
		end
	end
end

local function disconnectAll<T...>(self: Signal<T...>)
	local node = (self :: SignalInternal<T...>)._head
	while node do
		disconnect(node)
		node = node._next
	end
end

local function once<T...>(self: Signal<T...>, callback: (T...) -> ()): Connection<T...>
	local conn: Connection<T...>
	conn = connect(self, function(...)
		disconnect(conn)
		callback(...)
	end)
	return conn
end

local function wait<T...>(self: Signal<T...>): T...
	local cor = coroutine.running()
	local conn: Connection<T...>
	conn = connect(self, function(...)
		disconnect(conn)
		if coroutine.status(cor) == "suspended" then
			task.spawn(cor, ...)
		end
	end)
	return coroutine.yield()
end

local function fire<T...>(self: Signal<T...>, ...: T...)
	local node = (self :: SignalInternal<T...>)._head
	while node do
		if node._isSync then
			node._callback(...)
		else
			local thread
			local numThreads = #threads
			if numThreads > 0 then
				thread = threads[numThreads]
				threads[numThreads] = nil
			else
				thread = coroutine.create(yieldThread)
				coroutine.resume(thread)
			end	
			task.spawn(thread, node._callback, thread, ...)
		end
		node = node._next
	end
end

local function new<T...>(): Signal<T...>
	return setmetatable({_head = false}, Signal) :: Signal<T...>
end

Signal.Connect = connect
Signal.ConnectSync = connectSync
Signal.Fire = fire
Signal.Once = once
Signal.Wait = wait
Signal.DisconnectAll = disconnectAll
Connection.Disconnect = disconnect
table.freeze(Signal)
table.freeze(Connection)

return {new = new}
