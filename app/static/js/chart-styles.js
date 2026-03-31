/**
 * Chart Styles – High-Tech Platform Design System
 * Stili fighi per ogni tipo di grafico: doughnut 3D, bar gradient, line glow, KPI gauge.
 */
(function() {
  'use strict';

  var HIGHTECH = {
    palette: [
      '#00d4ff',  /* cyan */
      '#ff3d94',  /* magenta */
      '#ffb347',  /* amber/orange */
      '#00e676',  /* green */
      '#ff6b6b',  /* coral */
      '#b388ff',  /* violet */
      '#ffd54f',  /* yellow */
      '#4dd0e1'   /* light cyan */
    ],
    brand: '#FFD700',
    brandDim: 'rgba(255,215,0,0.5)',
    bg: '#0d0d0d',
    grid: 'rgba(0,212,255,0.08)',
    text: '#b0b0b0',
    textBright: '#e8e8e8'
  };

  function createGradient(ctx, x0, y0, x1, y1, colors) {
    var g = ctx.createLinearGradient(x0, y0, x1, y1);
    (colors || HIGHTECH.palette.slice(0, 3)).forEach(function(c, i) {
      g.addColorStop(i / Math.max(1, (colors || []).length - 1), c);
    });
    return g;
  }

  function createRadialGradient(ctx, x, y, r0, r1, inner, outer) {
    var g = ctx.createRadialGradient(x, y, r0, x, y, r1);
    g.addColorStop(0, inner || 'rgba(255,255,255,0.15)');
    g.addColorStop(1, outer || 'rgba(0,0,0,0.3)');
    return g;
  }

  /** Doughnut/Pie: premium look – yellow (brand) + gray (others) */
  function doughnut3DOptions(cutout) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      cutout: cutout != null ? String(cutout) : '68%',
      spacing: 4,
      borderRadius: 4,
      plugins: {
        legend: { display: false },
        tooltip: {
          enabled: true,
          backgroundColor: 'rgba(13,13,13,0.95)',
          titleColor: HIGHTECH.textBright,
          bodyColor: HIGHTECH.text,
          titleFont: { size: 14 },
          bodyFont: { size: 13 },
          borderColor: 'rgba(0,212,255,0.3)',
          borderWidth: 1,
          padding: 14,
          cornerRadius: 8,
          displayColors: true
        }
      },
      animation: { duration: 800, easing: 'easeOutQuart' },
      layout: { padding: 16 }
    };
  }

  /** Only two colors: yellow for brand, gray for all others */
  function doughnut3DColors(count, brandIndex) {
    var GRAY = 'rgba(90,90,90,0.85)';
    var colors = [];
    for (var i = 0; i < count; i++) {
      colors.push(brandIndex >= 0 && i === brandIndex ? HIGHTECH.brand : GRAY);
    }
    return colors;
  }

  /** Bar: gradienti, bordi arrotondati, glow */
  function barGradientOptions(indexAxis) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: indexAxis || 'x',
      barPercentage: 0.75,
      categoryPercentage: 0.85,
      borderRadius: 8,
      plugins: {
        legend: {
          display: true,
          position: 'top',
          labels: {
            color: HIGHTECH.textBright,
            font: { size: 15, weight: '600' },
            padding: 16,
            usePointStyle: true,
            pointStyle: 'circle'
          }
        },
        tooltip: {
          backgroundColor: 'rgba(13,13,13,0.95)',
          titleColor: HIGHTECH.textBright,
          bodyColor: HIGHTECH.text,
          titleFont: { size: 14 },
          bodyFont: { size: 13 },
          borderColor: 'rgba(0,212,255,0.3)',
          borderWidth: 1,
          padding: 14,
          cornerRadius: 8
        }
      },
      scales: {
        x: {
          grid: { color: HIGHTECH.grid, drawBorder: false },
          ticks: { color: HIGHTECH.text, font: { size: 12 }, maxRotation: 45, minRotation: 25 }
        },
        y: {
          grid: { color: HIGHTECH.grid, drawBorder: false },
          ticks: {
            color: HIGHTECH.text,
            font: { size: 14 },
            callback: function(v) {
              return v >= 1000000 ? (v/1e6).toFixed(1) + 'M' : v >= 1000 ? (v/1000).toFixed(1) + 'k' : v;
            }
          }
        }
      },
      animation: { duration: 600, easing: 'easeOutQuart' },
      layout: { padding: { top: 8, bottom: 8, left: 8, right: 8 } }
    };
  }

  /** Bar: yellow for brand, gray for others (category avg / competitor) */
  function barGradientDatasets(labels, brandData, mediaData, brandLabel, mediaLabel) {
    var GRAY = 'rgba(90,90,90,0.75)';
    return {
      labels: labels,
      datasets: [
        {
          label: brandLabel || 'Your Brand',
          data: brandData,
          backgroundColor: 'rgba(255,215,0,0.9)',
          borderColor: HIGHTECH.brand,
          borderWidth: 1,
          borderRadius: 8,
          hoverBackgroundColor: 'rgba(255,215,0,1)'
        },
        {
          label: mediaLabel || 'Category Avg',
          data: mediaData,
          backgroundColor: GRAY,
          borderColor: 'rgba(120,120,120,0.9)',
          borderWidth: 1,
          borderRadius: 8,
          hoverBackgroundColor: 'rgba(100,100,100,0.9)'
        }
      ]
    };
  }

  /** Line/Area: curve smooth, fill, glow */
  function lineGlowOptions() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      tension: 0.4,
      fill: true,
      pointRadius: 4,
      pointHoverRadius: 6,
      pointBackgroundColor: HIGHTECH.brand,
      pointBorderColor: '#0d0d0d',
      pointBorderWidth: 2,
      plugins: {
        legend: {
          display: true,
          position: 'top',
          labels: { color: HIGHTECH.textBright, font: { size: 14 }, usePointStyle: true }
        },
        tooltip: {
          backgroundColor: 'rgba(13,13,13,0.95)',
          borderColor: 'rgba(0,212,255,0.3)',
          cornerRadius: 8
        }
      },
      scales: {
        x: { grid: { color: HIGHTECH.grid }, ticks: { color: HIGHTECH.text } },
        y: { grid: { color: HIGHTECH.grid }, ticks: { color: HIGHTECH.text } }
      },
      animation: { duration: 700, easing: 'easeOutQuart' }
    };
  }

  /** KPI Gauge: stile circolare progress */
  function kpiGaugeStyle(value, max, color) {
    return {
      value: value,
      max: max || 100,
      color: color || HIGHTECH.brand
    };
  }

  /** Insight copy – English only */
  var INSIGHTS = {
    leader: function(brand) {
      return brand + ' leads in market share.';
    },
    rank: function(brand, ord, suf, leader, gap) {
      var s = brand + ' is #' + ord + suf + ' in ranking.';
      if (leader && gap) s += ' Leader ' + leader + ' has +' + gap + '% share.';
      return s;
    },
    shareUp: function(delta) {
      return 'Market share growing: +' + delta + 'pp vs previous year. Positive momentum.';
    },
    shareDown: function(delta) {
      return 'Market share declining: ' + delta + 'pp vs previous year.';
    },
    shareFlat: function() {
      return 'Market share stable. Position consolidation.';
    }
  };

  window.ChartStyles = {
    HIGHTECH: HIGHTECH,
    doughnut3DOptions: doughnut3DOptions,
    doughnut3DColors: doughnut3DColors,
    barGradientOptions: barGradientOptions,
    barGradientDatasets: barGradientDatasets,
    lineGlowOptions: lineGlowOptions,
    kpiGaugeStyle: kpiGaugeStyle,
    INSIGHTS: INSIGHTS,
    createGradient: createGradient,
    createRadialGradient: createRadialGradient
  };
})();
