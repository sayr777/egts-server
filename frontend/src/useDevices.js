import { useEffect, useRef, useState } from 'react'

// Merges live WebSocket updates into a device map keyed by device_id.
export function useDevices() {
  const [devices, setDevices] = useState({})   // { [device_id]: DeviceState }
  const [connected, setConnected] = useState(false)
  const wsRef = useRef(null)

  useEffect(() => {
    let reconnectTimer = null

    function connect() {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      const ws = new WebSocket(`${proto}://${location.host}/ws`)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        reconnectTimer = setTimeout(connect, 3000)
      }
      ws.onerror = () => ws.close()

      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data)
          if (msg.type === 'snapshot') {
            // Initial load: replace all
            const next = {}
            for (const d of msg.devices ?? []) next[d.device_id] = d
            setDevices(next)
          } else if (msg.device_id != null) {
            // Single live update
            setDevices(prev => ({ ...prev, [msg.device_id]: msg }))
          }
        } catch { /* ignore malformed */ }
      }
    }

    connect()
    return () => {
      clearTimeout(reconnectTimer)
      wsRef.current?.close()
    }
  }, [])

  return { devices, connected }
}
