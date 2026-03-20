import { 
  useGetSwarmStatus, 
  useGetSwarmConsensus 
} from "@workspace/api-client-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { format } from "date-fns";
import { Activity, Shield, TrendingUp, Zap, ServerCrash, Cpu } from "lucide-react";
import { ResponsiveContainer, AreaChart, Area, XAxis, Tooltip } from "recharts";
import { motion } from "framer-motion";

// Mock data for the sparkline chart
const mockChartData = Array.from({ length: 20 }).map((_, i) => ({
  time: i,
  pnl: Math.random() * 1000 - 200 + i * 50
}));

export default function Dashboard() {
  const { data: status, isLoading: isStatusLoading } = useGetSwarmStatus({
    query: { refetchInterval: 5000 }
  });
  
  const { data: consensus, isLoading: isConsensusLoading } = useGetSwarmConsensus({
    query: { refetchInterval: 5000 }
  });

  if (isStatusLoading || isConsensusLoading) {
    return (
      <div className="h-full flex items-center justify-center font-mono text-primary text-xl animate-pulse">
        [ INIT_SYSTEM_CORE... ]
      </div>
    );
  }

  if (!status || !consensus) return null;

  const isPositive = status.totalPnl >= 0;
  const isActionBuy = consensus.action.includes('BUY');
  const isActionSell = consensus.action.includes('SELL');

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      <header className="flex justify-between items-end border-b border-primary/20 pb-4">
        <div>
          <h2 className="text-3xl font-bold text-primary tracking-widest uppercase text-glow">Tactical Overview</h2>
          <p className="text-muted-foreground font-mono text-sm mt-1">
            SYS_TIME: {format(new Date(), 'HH:mm:ss.SSS')} | UPTIME: {(status.uptime / 3600).toFixed(1)}H
          </p>
        </div>
        <Badge variant="outline" className="border-primary text-primary animate-pulse-glow font-mono px-3 py-1">
          LIVE_LINK_ACTIVE
        </Badge>
      </header>

      {/* Primary Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard 
          title="SWARM_INTEGRITY" 
          value={`${status.healthScore.toFixed(1)}%`}
          icon={Shield}
          trend={status.healthScore > 90 ? "OPTIMAL" : "DEGRADED"}
          color="primary"
        />
        <StatCard 
          title="ACTIVE_AGENTS" 
          value={`${status.activeBots} / ${status.totalBots}`}
          icon={Activity}
          trend={`${status.crashedBots} OFFLINE`}
          color={status.crashedBots > 0 ? "destructive" : "primary"}
        />
        <StatCard 
          title="NET_PNL" 
          value={`${isPositive ? '+' : ''}$${status.totalPnl.toLocaleString()}`}
          icon={TrendingUp}
          trend={`${status.winRate.toFixed(1)}% WIN RATE`}
          color={isPositive ? "secondary" : "destructive"}
        />
        <StatCard 
          title="META_LEVEL" 
          value={`LVL ${status.xpLevel}`}
          icon={Zap}
          trend={`${status.totalXp.toLocaleString()} XP`}
          color="accent"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Consensus Widget */}
        <Card className="col-span-1 lg:col-span-1 bg-card/50 border-primary/30 box-glow backdrop-blur-sm relative overflow-hidden">
          <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,rgba(0,255,255,0.1)_0%,transparent_70%)] pointer-events-none" />
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest">GODMIND_CONSENSUS</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col items-center justify-center py-8">
            <motion.div 
              key={consensus.action}
              initial={{ scale: 0.8, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              className="mb-6 relative"
            >
              <div className={`text-6xl font-bold tracking-tighter ${
                isActionBuy ? 'text-secondary text-glow-secondary' : 
                isActionSell ? 'text-destructive text-glow-destructive' : 
                'text-primary text-glow'
              }`}>
                {consensus.action}
              </div>
            </motion.div>
            
            <div className="w-full max-w-[80%] space-y-2">
              <div className="flex justify-between text-xs font-mono text-muted-foreground">
                <span>CONFIDENCE</span>
                <span className="text-primary">{consensus.confidence.toFixed(1)}%</span>
              </div>
              <Progress value={consensus.confidence} className="h-2 bg-primary/20" />
            </div>

            <div className="mt-8 grid grid-cols-2 gap-8 w-full text-center font-mono">
              <div>
                <div className="text-xs text-muted-foreground">REGIME</div>
                <div className="text-primary text-lg">{consensus.dominantRegime}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">VOTES</div>
                <div className="text-primary text-lg">{consensus.votes} / {consensus.requiredVotes}</div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* PnL Chart */}
        <Card className="col-span-1 lg:col-span-2 bg-card/50 border-primary/30 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest">PNL_TRAJECTORY</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[300px] w-full mt-4">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={mockChartData}>
                  <defs>
                    <linearGradient id="colorPnl" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <Tooltip 
                    contentStyle={{ backgroundColor: 'hsl(var(--card))', borderColor: 'hsl(var(--primary))', fontFamily: 'var(--font-mono)' }}
                    itemStyle={{ color: 'hsl(var(--primary))' }}
                  />
                  <Area 
                    type="monotone" 
                    dataKey="pnl" 
                    stroke="hsl(var(--primary))" 
                    fillOpacity={1} 
                    fill="url(#colorPnl)" 
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Engine Votes Table */}
      <Card className="bg-card/50 border-primary/30 backdrop-blur-sm">
        <CardHeader>
          <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
            <Cpu className="w-4 h-4" />
            LIVE_ENGINE_TELEMETRY
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left font-mono">
              <thead className="text-xs text-primary uppercase border-b border-primary/30">
                <tr>
                  <th className="px-4 py-3">ENGINE_ID</th>
                  <th className="px-4 py-3">STRATEGY</th>
                  <th className="px-4 py-3">VOTE</th>
                  <th className="px-4 py-3">CONFIDENCE</th>
                  <th className="px-4 py-3 text-right">WEIGHT</th>
                </tr>
              </thead>
              <tbody>
                {consensus.engineVotes.map((vote, idx) => (
                  <tr key={vote.engineId} className="border-b border-primary/10 hover:bg-primary/5 transition-colors">
                    <td className="px-4 py-3 text-muted-foreground">{vote.engineId.substring(0,8)}</td>
                    <td className="px-4 py-3 text-primary-foreground">{vote.engineName}</td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-1 rounded-sm text-xs ${
                        vote.vote.includes('BUY') ? 'bg-secondary/20 text-secondary' : 
                        vote.vote.includes('SELL') ? 'bg-destructive/20 text-destructive' : 
                        'bg-primary/20 text-primary'
                      }`}>
                        {vote.vote}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-primary">{(vote.confidence * 100).toFixed(1)}%</td>
                    <td className="px-4 py-3 text-right text-muted-foreground">{vote.weight.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function StatCard({ title, value, icon: Icon, trend, color }: any) {
  const colorClasses = {
    primary: "text-primary border-primary/30 bg-primary/5",
    secondary: "text-secondary border-secondary/30 bg-secondary/5",
    destructive: "text-destructive border-destructive/30 bg-destructive/5",
    accent: "text-accent border-accent/30 bg-accent/5",
  };

  return (
    <Card className={`clip-edges backdrop-blur-md ${colorClasses[color as keyof typeof colorClasses]}`}>
      <CardContent className="p-6">
        <div className="flex justify-between items-start">
          <div className="space-y-2">
            <p className="font-mono text-xs text-muted-foreground uppercase tracking-wider">{title}</p>
            <p className="text-3xl font-bold tracking-wider text-glow font-sans">{value}</p>
          </div>
          <div className={`p-3 rounded-sm bg-background/50 border border-current opacity-80`}>
            <Icon className="w-5 h-5" />
          </div>
        </div>
        <div className="mt-4 text-xs font-mono opacity-80 uppercase tracking-widest border-t border-current pt-2">
          {trend}
        </div>
      </CardContent>
    </Card>
  );
}
