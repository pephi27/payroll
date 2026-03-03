(function(){
  if (window.__bpSafeDom) return;
  window.__bpSafeDom = {
    byId: function(id){ return document.getElementById(id); },
    on: function(el, ev, fn, opts){ if (!el || !el.addEventListener) return false; el.addEventListener(ev, fn, opts); return true; },
    val: function(el){ return el && typeof el.value !== 'undefined' ? el.value : ''; }
  };
})();
