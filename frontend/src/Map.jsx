import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'

const MAP_STYLE = 'https://tiles.openfreemap.org/styles/liberty'

export default function Map({ devices }) {
  const containerRef = useRef(null)
  const mapRef      = useRef(null)
  const markersRef  = useRef({})  // device_id → maplibregl.Marker

  // Init map once
  useEffect(() => {
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: [37.62, 55.75],
      zoom: 9,
    })
    map.addControl(new maplibregl.NavigationControl(), 'top-right')
    mapRef.current = map
    return () => map.remove()
  }, [])

  // Sync markers when devices change
  useEffect(() => {
    const map = mapRef.current
    if (!map) return

    const seen = new Set()

    for (const d of Object.values(devices)) {
      const id = d.device_id
      seen.add(id)

      const lngLat = [d.lon, d.lat]

      if (markersRef.current[id]) {
        markersRef.current[id].setLngLat(lngLat)
      } else {
        const el = document.createElement('div')
        el.className = 'device-marker'
        el.style.cssText = [
          'width:14px', 'height:14px', 'border-radius:50%',
          'background:#00c8ff', 'border:2px solid #fff',
          'box-shadow:0 0 6px rgba(0,200,255,0.8)', 'cursor:pointer',
        ].join(';')

        const popup = new maplibregl.Popup({ offset: 12 }).setHTML(
          `<b>Device ${id}</b><br/>` +
          `Lat: ${d.lat?.toFixed(5)}<br/>` +
          `Lon: ${d.lon?.toFixed(5)}<br/>` +
          `Speed: ${d.speed} km/h`
        )

        markersRef.current[id] = new maplibregl.Marker({ element: el })
          .setLngLat(lngLat)
          .setPopup(popup)
          .addTo(map)
      }
    }

    // Remove markers for devices no longer in the list
    for (const id of Object.keys(markersRef.current)) {
      if (!seen.has(Number(id))) {
        markersRef.current[id].remove()
        delete markersRef.current[id]
      }
    }
  }, [devices])

  return <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
}
