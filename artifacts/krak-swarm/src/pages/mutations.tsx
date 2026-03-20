import { useState } from "react";
import { 
  useGetEliteStrategies, 
  useGetMutationHistory,
  useRunMutation,
  getGetMutationHistoryQueryKey
} from "@workspace/api-client-react";
import { useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { format } from "date-fns";
import { Dna, Zap, History, Target, AlertCircle } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import type { MutationRequestStrategy } from "@workspace/api-client-react/src/generated/api.schemas";

export default function Mutations() {
  const { data: elites, isLoading: elitesLoading } = useGetEliteStrategies();
  const { data: history, isLoading: historyLoading } = useGetMutationHistory({ limit: 10 });
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [strategy, setStrategy] = useState<MutationRequestStrategy>("genetic");
  const [rate, setRate] = useState([0.15]);

  const mutation = useRunMutation({
    mutation: {
      onSuccess: (data) => {
        toast({ 
          title: "Evolution Cycle Complete", 
          description: `Mutated ${data.mutationsApplied} params. Best Δ: +${data.bestFitnessDelta.toFixed(3)}` 
        });
        queryClient.invalidateQueries({ queryKey: getGetMutationHistoryQueryKey() });
      }
    }
  });

  const handleRunMutation = () => {
    mutation.mutate({
      data: {
        strategy,
        mutationRate: rate[0],
        populationSize: 25
      }
    });
  };

  return (
    <div className="space-y-6">
      <header className="border-b border-primary/20 pb-4">
        <h2 className="text-3xl font-bold text-primary tracking-widest uppercase text-glow">Evolution_Matrix</h2>
        <p className="text-muted-foreground font-mono text-sm mt-1">
          MAP-ELITES ARCHIVE & PARAMETER MUTATION CONTROLS
        </p>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* Mutation Control Panel */}
        <Card className="lg:col-span-1 bg-card/40 border-primary/30 shadow-[0_0_30px_rgba(0,255,255,0.05)] border-t-primary">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
              <Dna className="w-4 h-4" />
              INITIATE_MUTATION_CYCLE
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6 font-mono text-sm">
            <div className="space-y-3">
              <label className="text-muted-foreground text-xs uppercase">Algorithm_Strategy</label>
              <Select value={strategy} onValueChange={(v) => setStrategy(v as MutationRequestStrategy)}>
                <SelectTrigger className="bg-background border-primary/30 text-primary">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-card border-primary/50 text-primary font-mono text-xs">
                  <SelectItem value="genetic">Standard Genetic (GA)</SelectItem>
                  <SelectItem value="map_elites">MAP-Elites (Diversity)</SelectItem>
                  <SelectItem value="guided">Guided Local Search</SelectItem>
                  <SelectItem value="random">Random Entropy</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-4">
              <div className="flex justify-between">
                <label className="text-muted-foreground text-xs uppercase">Mutation_Rate</label>
                <span className="text-primary">{(rate[0] * 100).toFixed(0)}%</span>
              </div>
              <Slider 
                value={rate} 
                onValueChange={setRate} 
                max={0.5} 
                step={0.01}
                className="py-2"
              />
            </div>

            <div className="bg-primary/5 p-3 border border-primary/20 rounded-sm">
              <div className="flex gap-2 items-start text-xs text-muted-foreground">
                <AlertCircle className="w-4 h-4 text-primary shrink-0" />
                <p>This will permanently alter underlying neural architectures. Unsuccessful mutations will be pruned.</p>
              </div>
            </div>

            <Button 
              className="w-full bg-primary text-primary-foreground hover:bg-primary/80 box-glow tracking-widest uppercase font-bold"
              onClick={handleRunMutation}
              disabled={mutation.isPending}
            >
              {mutation.isPending ? "EVOLVING..." : "EXECUTE_MUTATION"}
            </Button>
          </CardContent>
        </Card>

        {/* Elite Strategies Grid */}
        <Card className="lg:col-span-2 bg-card/40 border-primary/30">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
              <Target className="w-4 h-4" />
              MAP_ELITES_ARCHIVE
            </CardTitle>
          </CardHeader>
          <CardContent>
            {elitesLoading ? (
              <div className="font-mono text-primary animate-pulse text-center py-8">[ LOADING_ELITES... ]</div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {elites?.elites.map(elite => (
                  <div key={elite.id} className="border border-primary/20 bg-background/50 p-4 relative overflow-hidden group">
                    <div className="absolute top-0 left-0 w-full h-[1px] bg-gradient-to-r from-transparent via-primary/50 to-transparent" />
                    <div className="flex justify-between items-start mb-2">
                      <div className="font-mono text-xs text-primary-foreground font-bold">{elite.niche}</div>
                      <Badge variant="outline" className="border-accent text-accent text-[10px] h-5">{elite.regime}</Badge>
                    </div>
                    <div className="flex justify-between text-xs font-mono mb-4">
                      <span className="text-muted-foreground">FITNESS:</span>
                      <span className="text-secondary text-glow-secondary font-bold">{elite.fitness.toFixed(4)}</span>
                    </div>
                    <div className="grid grid-cols-2 gap-x-2 gap-y-1 text-[10px] font-mono text-muted-foreground">
                      {Object.entries(elite.params).slice(0, 4).map(([k, v]) => (
                        <div key={k} className="flex justify-between border-b border-primary/10 pb-1">
                          <span className="truncate mr-2">{k}</span>
                          <span className="text-primary">{Number(v).toFixed(4)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Mutation History Table */}
      <Card className="bg-card/40 border-primary/30">
        <CardHeader>
          <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
            <History className="w-4 h-4" />
            MUTATION_LOG
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm font-mono text-left">
              <thead className="bg-primary/5 text-xs text-primary border-b border-primary/30 uppercase tracking-wider">
                <tr>
                  <th className="px-4 py-3">TIMESTAMP</th>
                  <th className="px-4 py-3">STRATEGY</th>
                  <th className="px-4 py-3">PARAMS_MUTATED</th>
                  <th className="px-4 py-3 text-secondary">IMPROVED</th>
                  <th className="px-4 py-3 text-destructive">DEGRADED</th>
                  <th className="px-4 py-3 text-right text-primary">BEST_DELTA</th>
                </tr>
              </thead>
              <tbody>
                {historyLoading ? (
                  <tr><td colSpan={6} className="text-center py-8 text-primary animate-pulse">[ READING_LOGS... ]</td></tr>
                ) : history?.mutations.map((mut) => (
                  <tr key={mut.id} className="border-b border-primary/10 hover:bg-primary/5 transition-colors">
                    <td className="px-4 py-3 text-muted-foreground">{format(new Date(mut.timestamp), "HH:mm:ss.SSS")}</td>
                    <td className="px-4 py-3 text-primary-foreground">{mut.strategy}</td>
                    <td className="px-4 py-3 text-center">{mut.mutationsApplied}</td>
                    <td className="px-4 py-3 text-secondary">{mut.improved}</td>
                    <td className="px-4 py-3 text-destructive">{mut.degraded}</td>
                    <td className={`px-4 py-3 text-right font-bold ${mut.bestFitnessDelta > 0 ? 'text-secondary' : 'text-destructive'}`}>
                      {mut.bestFitnessDelta > 0 ? '+' : ''}{mut.bestFitnessDelta.toFixed(4)}
                    </td>
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
