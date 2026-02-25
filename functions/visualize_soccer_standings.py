import logging

logger = logging.getLogger(__name__)


def main(spark):
    """
    Create a fun Premier League dashboard with standings, trends, and stats.
    """
    df = spark.table("analytics.soccer_standings")
    rows = df.collect()
    
    logger.info(f"Visualizing {len(rows)} teams")
    
    # Convert to dicts for viz
    teams = [
        {
            "position": int(row.position),
            "team": row.team_name,
            "points": int(row.points),
            "played": int(row.played),
            "won": int(row.won),
            "drawn": int(row.drawn),
            "lost": int(row.lost),
            "gf": int(row.goals_for),
            "ga": int(row.goals_against),
            "gd": int(row.goal_difference),
            "win_pct": float(row.wins_pct),
        }
        for row in rows
    ]
    
    # Top 6 (Champions League spots)
    top_6 = teams[:6]
    
    # Relegation zone (bottom 3)
    relegation = teams[-3:]
    
    # All teams for full table
    all_teams = sorted(teams, key=lambda x: x["position"])
    
    viz_specs = [
        # 1. League Leaders (Top 6)
        {
            "type": "metric",
            "title": "🏆 League Leader",
            "value": teams[0]["team"],
        },
        {
            "type": "metric",
            "title": f"🥇 Points (1st place)",
            "value": teams[0]["points"],
        },
        
        # 2. Points Race (Top 10)
        {
            "type": "bar",
            "title": "⚽ Premier League Title Race (Top 10)",
            "data": top_6 + teams[6:10],
            "xKey": "team",
            "series": [
                {
                    "key": "points",
                    "label": "Points",
                    "color": "#10b981",  # Green
                }
            ],
        },
        
        # 3. Win % Leaders
        {
            "type": "bar",
            "title": "🎯 Win Percentage Leaders",
            "data": sorted(teams[:10], key=lambda x: x["win_pct"], reverse=True),
            "xKey": "team",
            "series": [
                {
                    "key": "win_pct",
                    "label": "Win %",
                    "color": "#3b82f6",  # Blue
                }
            ],
        },
        
        # 4. Goal Difference (Attack vs Defense)
        {
            "type": "bar",
            "title": "⚔️ Goal Difference Leaders (Top 8)",
            "data": sorted(top_6 + teams[6:8], key=lambda x: x["gd"], reverse=True),
            "xKey": "team",
            "series": [
                {
                    "key": "gf",
                    "label": "Goals For",
                    "color": "#10b981",  # Green (attack)
                },
                {
                    "key": "ga",
                    "label": "Goals Against",
                    "color": "#ef4444",  # Red (defense)
                }
            ],
        },
        
        # 5. Full Table
        {
            "type": "table",
            "title": "📊 Full Premier League Standings",
            "columns": ["position", "team", "played", "won", "drawn", "lost", "points", "gd"],
            "data": all_teams,
        },
        
        # 6. Relegation Zone Alert
        {
            "type": "table",
            "title": "⚠️ Relegation Zone (Bottom 3)",
            "columns": ["position", "team", "played", "points", "won", "lost", "gd"],
            "data": relegation,
        },
        
        # 7. Most Goals Scored
        {
            "type": "bar",
            "title": "⚡ Top Goal Scorers (by Team)",
            "data": sorted(teams, key=lambda x: x["gf"], reverse=True)[:8],
            "xKey": "team",
            "series": [
                {
                    "key": "gf",
                    "label": "Goals Scored",
                    "color": "#fbbf24",  # Yellow
                }
            ],
        },
        
        # 8. Best Defenses
        {
            "type": "bar",
            "title": "🛡️ Best Defenses (Fewest Goals Conceded)",
            "data": sorted(teams, key=lambda x: x["ga"])[:8],
            "xKey": "team",
            "series": [
                {
                    "key": "ga",
                    "label": "Goals Conceded",
                    "color": "#ef4444",  # Red
                }
            ],
        },
    ]
    
    logger.info(f"Generated dashboard with {len(viz_specs)} visualizations")
    return viz_specs
