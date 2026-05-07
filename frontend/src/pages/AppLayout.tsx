import { useEffect, useRef, useState } from 'react'
import { NavLink, Outlet, useLocation } from 'react-router-dom'

export default function AppLayout() {
  const location = useLocation()
  const [groupOpen, setGroupOpen] = useState(false)
  const [trendOpen, setTrendOpen] = useState(false)
  const [adsOpen, setAdsOpen] = useState(false)
  const groupRef = useRef<HTMLDivElement | null>(null)
  const trendRef = useRef<HTMLDivElement | null>(null)
  const adsRef = useRef<HTMLDivElement | null>(null)

  const trendingSubPaths =
    location.pathname === '/trend' ||
    location.pathname === '/trend/session-impression' ||
    location.pathname === '/trend/session&impression' ||
    location.pathname === '/trend/New Listing'
  const trendingNavActive = trendingSubPaths || trendOpen

  const adsSubPaths = location.pathname.startsWith('/ads/')
  const adsNavActive = adsSubPaths || adsOpen

  useEffect(() => {
    const onDocMouseDown = (e: MouseEvent) => {
      const t = e.target as Node
      if (groupRef.current && !groupRef.current.contains(t)) {
        setGroupOpen(false)
      }
      if (trendRef.current && !trendRef.current.contains(t)) {
        setTrendOpen(false)
      }
      if (adsRef.current && !adsRef.current.contains(t)) {
        setAdsOpen(false)
      }
    }
    document.addEventListener('mousedown', onDocMouseDown)
    return () => document.removeEventListener('mousedown', onDocMouseDown)
  }, [])

  return (
    <div className="app-shell">
      <nav className="top-nav">
        <div className="top-nav-group" ref={groupRef}>
          <button
            type="button"
            className={`top-nav-link top-nav-group-toggle ${groupOpen ? 'is-active' : ''}`}
            onClick={() => setGroupOpen((v) => !v)}
          >
            Group
          </button>
          <div className={`top-nav-menu ${groupOpen ? 'is-open' : ''}`}>
            <NavLink to="/" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>S</NavLink>
            <NavLink to="/group/A" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>A</NavLink>
            <NavLink to="/group/B" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>B</NavLink>
            <NavLink to="/group/F" className="top-nav-menu-link" onClick={() => setGroupOpen(false)}>F</NavLink>
          </div>
        </div>
        <NavLink to="/tasks" className={({ isActive }) => `top-nav-link ${isActive ? 'is-active' : ''}`}>
          Tasks
        </NavLink>
        <NavLink to="/monitor" className={({ isActive }) => `top-nav-link ${isActive ? 'is-active' : ''}`}>
          Monitor
        </NavLink>
        <div className="top-nav-group" ref={adsRef}>
          <button
            type="button"
            className={`top-nav-link top-nav-group-toggle ${adsNavActive ? 'is-active' : ''}`}
            onClick={() => setAdsOpen((v) => !v)}
            aria-expanded={adsOpen}
            aria-haspopup="menu"
          >
            Ads
          </button>
          <div className={`top-nav-menu ${adsOpen ? 'is-open' : ''}`}>
            <NavLink
              to="/ads/ad-sales"
              className="top-nav-menu-link"
              onClick={() => setAdsOpen(false)}
            >
              Ad-Sales
            </NavLink>
            <NavLink
              to="/ads/profit"
              className="top-nav-menu-link"
              onClick={() => setAdsOpen(false)}
            >
              Total Profit
            </NavLink>
          </div>
        </div>
        <div className="top-nav-group" ref={trendRef}>
          <button
            type="button"
            className={`top-nav-link top-nav-group-toggle ${trendingNavActive ? 'is-active' : ''}`}
            onClick={() => setTrendOpen((v) => !v)}
            aria-expanded={trendOpen}
            aria-haspopup="menu"
          >
            Trending
          </button>
          <div className={`top-nav-menu top-nav-menu--wide ${trendOpen ? 'is-open' : ''}`}>
            <NavLink
              to="/trend"
              className="top-nav-menu-link"
              onClick={() => setTrendOpen(false)}
            >
              Weekly trend
            </NavLink>
            <NavLink
              to="/trend/session-impression"
              className="top-nav-menu-link"
              onClick={() => setTrendOpen(false)}
            >
              session & impression
            </NavLink>
            <NavLink
              to="/trend/New Listing"
              className="top-nav-menu-link"
              onClick={() => setTrendOpen(false)}
            >
              New Listing
            </NavLink>
          </div>
        </div>
      </nav>
      <div className="app-shell-content">
        <Outlet />
      </div>
    </div>
  )
}
