def ic(name: str, color: str = "#64748b", size: int = 16) -> str:
    s, c = size, color
    st = f'stroke="{c}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"'
    b  = f'width="{s}" height="{s}" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;display:inline-block;flex-shrink:0;"'
    paths = {
        "home":           f'<path {st} d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline {st} points="9 22 9 12 15 12 15 22"/>',
        "search":         f'<circle cx="11" cy="11" r="8" {st}/><line x1="21" y1="21" x2="16.65" y2="16.65" {st}/>',
        "info":           f'<circle cx="12" cy="12" r="10" {st}/><line x1="12" y1="16" x2="12" y2="12" {st}/><line x1="12" y1="8" x2="12.01" y2="8" {st}/>',
        "settings":       f'<circle cx="12" cy="12" r="3" {st}/><path {st} d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
        "arrow-right":    f'<line x1="5" y1="12" x2="19" y2="12" {st}/><polyline points="12 5 19 12 12 19" {st}/>',
        "external-link":  f'<path {st} d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline {st} points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3" {st}/>',
        "briefcase":      f'<rect x="2" y="7" width="20" height="14" rx="2" ry="2" {st}/><path {st} d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/><path {st} d="M2 12h20"/>',
        "user":           f'<path {st} d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4" {st}/>',
        "users":          f'<path {st} d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" {st}/><path {st} d="M23 21v-2a4 4 0 0 0-3-3.87"/><path {st} d="M16 3.13a4 4 0 0 1 0 7.75"/>',
        "file-text":      f'<path {st} d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline {st} points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13" {st}/><line x1="16" y1="17" x2="8" y2="17" {st}/>',
        "upload":         f'<polyline {st} points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21" {st}/><path {st} d="M20.39 18.39A5 5 0 0 0 18 9h-1.26A8 8 0 1 0 3 16.3"/>',
        "target":         f'<circle cx="12" cy="12" r="10" {st}/><circle cx="12" cy="12" r="6" {st}/><circle cx="12" cy="12" r="2" {st}/>',
        "star":           f'<polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" {st}/>',
        "map-pin":        f'<path {st} d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3" {st}/>',
        "globe":          f'<circle cx="12" cy="12" r="10" {st}/><line x1="2" y1="12" x2="22" y2="12" {st}/><path {st} d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>',
        "database":       f'<ellipse cx="12" cy="5" rx="9" ry="3" {st}/><path {st} d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path {st} d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>',
        "bar-chart":      f'<line x1="12" y1="20" x2="12" y2="10" {st}/><line x1="18" y1="20" x2="18" y2="4" {st}/><line x1="6" y1="20" x2="6" y2="16" {st}/>',
        "trending-up":    f'<polyline points="23 6 13.5 15.5 8.5 10.5 1 18" {st}/><polyline points="17 6 23 6 23 12" {st}/>',
        "layers":         f'<polygon points="12 2 2 7 12 12 22 7 12 2" {st}/><polyline points="2 17 12 22 22 17" {st}/><polyline points="2 12 12 17 22 12" {st}/>',
        "code":           f'<polyline points="16 18 22 12 16 6" {st}/><polyline points="8 6 2 12 8 18" {st}/>',
        "zap":            f'<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" {st}/>',
        "activity":       f'<polyline points="22 12 18 12 15 21 9 3 6 12 2 12" {st}/>',
        "brain":          f'<path {st} d="M9.5 2A2.5 2.5 0 0 1 12 4.5v15a2.5 2.5 0 0 1-4.96-.44 2.5 2.5 0 0 1-2.96-3.08 3 3 0 0 1-.34-5.58 2.5 2.5 0 0 1 1.32-4.24 2.5 2.5 0 0 1 1.98-3A2.5 2.5 0 0 1 9.5 2z"/><path {st} d="M14.5 2A2.5 2.5 0 0 0 12 4.5v15a2.5 2.5 0 0 0 4.96-.44 2.5 2.5 0 0 0 2.96-3.08 3 3 0 0 0 .34-5.58 2.5 2.5 0 0 0-1.32-4.24 2.5 2.5 0 0 0-1.98-3A2.5 2.5 0 0 0 14.5 2z"/>',
        "git-branch":     f'<line x1="6" y1="3" x2="6" y2="15" {st}/><circle cx="18" cy="6" r="3" {st}/><circle cx="6" cy="18" r="3" {st}/><path {st} d="M18 9a9 9 0 0 1-9 9"/>',
        "refresh-cw":     f'<polyline points="23 4 23 10 17 10" {st}/><polyline points="1 20 1 14 7 14" {st}/><path {st} d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>',
        "clock":          f'<circle cx="12" cy="12" r="10" {st}/><polyline points="12 6 12 12 16 14" {st}/>',
        "package":        f'<line x1="16.5" y1="9.4" x2="7.5" y2="4.21" {st}/><path {st} d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline {st} points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12" {st}/>',
        "check-circle":   f'<path {st} d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline {st} points="22 4 12 14.01 9 11.01"/>',
        "x-circle":       f'<circle cx="12" cy="12" r="10" {st}/><line x1="15" y1="9" x2="9" y2="15" {st}/><line x1="9" y1="9" x2="15" y2="15" {st}/>',
        "alert-triangle": f'<path {st} d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13" {st}/><line x1="12" y1="17" x2="12.01" y2="17" {st}/>',
        "download":       f'<path {st} d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline {st} points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3" {st}/>',
        "link":           f'<path {st} d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path {st} d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>',
        "mail":           f'<path {st} d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline {st} points="22,6 12,13 2,6"/>',
        "rocket":         f'<path {st} d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path {st} d="M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>',
        "cpu":            f'<rect x="4" y="4" width="16" height="16" rx="2" ry="2" {st}/><rect x="9" y="9" width="6" height="6" {st}/>',
        "filter":         f'<polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" {st}/>',
    }
    inner = paths.get(name, f'<rect x="3" y="3" width="18" height="18" rx="2" {st}/>')
    return f'<svg {b}>{inner}</svg>'