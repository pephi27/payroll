    // Non-invasive performance tweaks: lazy-load offscreen media, async decode images.
    const onReady = (cb) => {
      if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', cb, { once: true });
      else cb();
    };
    onReady(() => {
      try {
        const vh = window.innerHeight || document.documentElement.clientHeight || 800;
        document.querySelectorAll('img').forEach(img => {
          try { if (!img.hasAttribute('decoding')) img.setAttribute('decoding', 'async'); } catch {}
          if (!img.hasAttribute('loading')) {
            const rect = img.getBoundingClientRect();
            if (rect && rect.top > vh) { try { img.setAttribute('loading', 'lazy'); } catch {} }
          }
        });
        document.querySelectorAll('iframe').forEach(el => {
          if (!el.hasAttribute('loading')) { try { el.setAttribute('loading', 'lazy'); } catch {} }
        });
      } catch { /* no-op */ }
    });
