import { useState } from "react";
import { useListBots } from "@workspace/api-client-react";
import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Bot, ChevronLeft, ChevronRight, Activity } from "lucide-react";
import type { ListBotsStatus } from "@workspace/api-client-react/src/generated/api.schemas";

export default function Bots() {
  const [page, setPage] = useState(0);
  const [statusFilter, setStatusFilter] = useState<ListBotsStatus | "ALL">("ALL");
  const limit = 20;

  const { data, isLoading } = useListBots({
    limit,
    offset: page * limit,
    ...(statusFilter !== "ALL" ? { status: statusFilter } : {})
  });

  const getStatusColor = (status: string) => {
    switch(status) {
      case 'active': return 'bg-secondary/20 text-secondary border-secondary/50';
      case 'idle': return 'bg-muted/50 text-muted-foreground border-muted';
      case 'crashed': return 'bg-destructive/20 text-destructive border-destructive/50';
      case 'healing': return 'bg-accent/20 text-accent border-accent/50 animate-pulse';
      default: return 'bg-primary/20 text-primary border-primary/50';
    }
  };

  return (
    <div className="space-y-6">
      <header className="border-b border-primary/20 pb-4 flex justify-between items-end">
        <div>
          <h2 className="text-3xl font-bold text-primary tracking-widest uppercase text-glow">Swarm_Entities</h2>
          <p className="text-muted-foreground font-mono text-sm mt-1">
            TOTAL_INDEXED: {data?.total || 0}
          </p>
        </div>
        
        <div className="w-48 font-mono">
          <Select 
            value={statusFilter} 
            onValueChange={(val) => { setStatusFilter(val as any); setPage(0); }}
          >
            <SelectTrigger className="bg-card border-primary/30 text-primary focus:ring-primary h-8 text-xs">
              <SelectValue placeholder="FILTER_STATUS" />
            </SelectTrigger>
            <SelectContent className="bg-card border-primary/50 font-mono text-xs text-primary">
              <SelectItem value="ALL">ALL_STATUSES</SelectItem>
              <SelectItem value="active">ACTIVE</SelectItem>
              <SelectItem value="idle">IDLE</SelectItem>
              <SelectItem value="crashed">CRASHED</SelectItem>
              <SelectItem value="healing">HEALING</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </header>

      <Card className="bg-card/40 border-primary/30 backdrop-blur-sm overflow-hidden">
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-sm font-mono text-left">
              <thead className="bg-primary/5 text-xs text-primary border-b border-primary/30 uppercase tracking-wider">
                <tr>
                  <th className="px-6 py-4">BOT_IDENTIFIER</th>
                  <th className="px-6 py-4">STATUS</th>
                  <th className="px-6 py-4">STRATEGY_HASH</th>
                  <th className="px-6 py-4 text-right">NET_PNL</th>
                  <th className="px-6 py-4 text-right">WIN_RATE</th>
                  <th className="px-6 py-4 text-right">FITNESS</th>
                  <th className="px-6 py-4 text-right">GEN</th>
                </tr>
              </thead>
              <tbody>
                {isLoading ? (
                  <tr>
                    <td colSpan={7} className="px-6 py-12 text-center text-primary animate-pulse">
                      [ FETCHING_SWARM_DATA... ]
                    </td>
                  </tr>
                ) : data?.bots.map((bot) => (
                  <tr key={bot.id} className="border-b border-primary/10 hover:bg-primary/5 transition-colors group">
                    <td className="px-6 py-3">
                      <div className="flex items-center gap-2">
                        <Bot className="w-4 h-4 text-muted-foreground group-hover:text-primary transition-colors" />
                        <div>
                          <div className="text-primary-foreground">{bot.name}</div>
                          <div className="text-[10px] text-muted-foreground">{bot.id.substring(0, 12)}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-3">
                      <Badge variant="outline" className={`text-[10px] ${getStatusColor(bot.status)}`}>
                        {bot.status}
                      </Badge>
                    </td>
                    <td className="px-6 py-3 text-muted-foreground text-xs">{bot.strategy.substring(0,16)}...</td>
                    <td className={`px-6 py-3 text-right font-bold ${bot.pnl >= 0 ? 'text-secondary' : 'text-destructive'}`}>
                      {bot.pnl >= 0 ? '+' : ''}${bot.pnl.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits:2})}
                    </td>
                    <td className="px-6 py-3 text-right text-primary-foreground">{bot.winRate.toFixed(1)}%</td>
                    <td className="px-6 py-3 text-right text-accent">{bot.fitness.toFixed(3)}</td>
                    <td className="px-6 py-3 text-right text-muted-foreground">G{bot.generation}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
        <div className="p-4 border-t border-primary/20 flex items-center justify-between bg-primary/5">
          <div className="font-mono text-xs text-muted-foreground">
            PAGE {page + 1} OF {Math.ceil((data?.total || 0) / limit)}
          </div>
          <div className="flex gap-2">
            <Button 
              variant="outline" 
              size="sm" 
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0 || isLoading}
              className="font-mono text-xs border-primary/30 text-primary hover:bg-primary hover:text-primary-foreground"
            >
              <ChevronLeft className="w-4 h-4 mr-1" /> PREV
            </Button>
            <Button 
              variant="outline" 
              size="sm" 
              onClick={() => setPage(p => p + 1)}
              disabled={!data || (page + 1) * limit >= data.total || isLoading}
              className="font-mono text-xs border-primary/30 text-primary hover:bg-primary hover:text-primary-foreground"
            >
              NEXT <ChevronRight className="w-4 h-4 ml-1" />
            </Button>
          </div>
        </div>
      </Card>
    </div>
  );
}
