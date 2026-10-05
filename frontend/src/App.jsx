import { useMemo } from 'react'
import Map from './Map'
import { useDevices } from './useDevices'

const styles = {
  root: {
    display: 'flex', flexDirection: 'column',
    width: '100%', height: '100%', overflow: 'hidden',
  },
  statusBar: {
    display: 'flex', alignItems: 'center', gap: 24,
    padding: '6px 16px',
    background: '#1a1a2e', borderBottom: '1px solid #333',
    fontSize: 13, flexShrink: 0,
  },
  dot: (ok) => ({
    display: 'inline-block', width: 8, height: 8, borderRadius: '50%',
    background: ok ? '#4caf50' : '#f44336', marginRight: 6,
  }),
  mapWrap: { flex: 1, position: 'relative' },
}

export default function App() {
  const { devices, connected } = useDevices()

  const stats = useMemo(() => {
    const list = Object.values(devices)
    const moving = list.filter(d => (d.speed ?? 0) > 0).length
    return { total: list.length, moving }
  }, [devices])

  return (
    <div style={styles.root}>
      <div style={styles.statusBar}>
        <span>
          <span style={styles.dot(connected)} />
          {connected ? 'Live' : 'Reconnecting…'}
        </span>
        <span>Devices online: <b>{stats.total}</b></span>
        <span>Moving: <b>{stats.moving}</b></span>
      </div>
      <div style={styles.mapWrap}>
        <Map devices={devices} />
      </div>
    </div>
  )
}
