import { useState } from "react";
import { 
  useGetHealingStatus, 
  useListErrors,
  useListHealActions,
  useTriggerHeal,
  getListErrorsQueryKey,
  getListHealActionsQueryKey
} from "@workspace/api-client-react";
import { useQueryClient } from "@tanstack/react-query";
import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Input } from "@/components/ui/input";
import { ShieldAlert, AlertOctagon, Terminal, Play, CheckCircle2, XCircle } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

export default function Healing() {
  const { data: status, isLoading: isStatusLoading } = useGetHealingStatus();
  const { data: errors, isLoading: isErrorsLoading } = useListErrors({ limit: 10 });
  const { data: actions, isLoading: isActionsLoading } = useListHealActions({ limit: 10 });
  
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [moduleInput, setModuleInput] = useState("");

  const triggerMutation = useTriggerHeal({
    mutation: {
      onSuccess: (res) => {
        toast({
          title: "Heal Routine Executed",
          description: `Result: ${res.result} | Strategy: ${res.strategy} | XP: +${res.xpEarned}`
        });
        queryClient.invalidateQueries({ queryKey: getListErrorsQueryKey() });
        queryClient.invalidateQueries({ queryKey: getListHealActionsQueryKey() });
      }
    }
  });

  const handleManualHeal = (e: React.FormEvent) => {
    e.preventDefault();
    if (!moduleInput) return;
    triggerMutation.mutate({ data: { module: moduleInput, strategy: "AST_REPAIR" } });
  };

  if (isStatusLoading) return <div className="font-mono text-primary animate-pulse text-xl">[ LOAD_HEALING_CORE... ]</div>;

  const xpProgress = status ? (status.totalXp / (status.totalXp + status.xpToNextLevel)) * 100 : 0;

  return (
    <div className="space-y-6">
      <header className="border-b border-primary/20 pb-4">
        <h2 className="text-3xl font-bold text-primary tracking-widest uppercase text-glow">Self_Healing_Protocol</h2>
        <p className="text-muted-foreground font-mono text-sm mt-1">
          AST-LEVEL AUTO-REPAIR & CIRCUIT BREAKERS
        </p>
      </header>

      {/* Status Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        
        <Card className="bg-card/40 border-primary/30">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
              <ShieldAlert className="w-4 h-4" />
              HEALING_META_STATUS
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <div>
              <div className="flex justify-between font-mono text-sm mb-2">
                <span className="text-muted-foreground">ENGINE_LEVEL {status?.xpLevel}</span>
                <span className="text-accent text-glow">{status?.totalXp.toLocaleString()} XP</span>
              </div>
              <Progress value={xpProgress} className="h-2 bg-accent/20" />
              <div className="text-right text-[10px] font-mono text-muted-foreground mt-1">
                {status?.xpToNextLevel.toLocaleString()} XP TO NEXT LEVEL
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 font-mono text-sm">
              <div className="border border-primary/20 bg-background/50 p-3">
                <div className="text-muted-foreground text-xs">SUCCESS_RATE</div>
                <div className="text-2xl font-bold text-secondary text-glow-secondary mt-1">
                  {(status?.healSuccessRate ?? 0 * 100).toFixed(1)}%
                </div>
              </div>
              <div className="border border-primary/20 bg-background/50 p-3">
                <div className="text-muted-foreground text-xs">UNRESOLVED_ERRS</div>
                <div className={`text-2xl font-bold mt-1 ${status && status.unresolvedErrors > 0 ? 'text-destructive text-glow-destructive' : 'text-primary'}`}>
                  {status?.unresolvedErrors}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-card/40 border-primary/30">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-primary tracking-widest flex items-center gap-2">
              <Terminal className="w-4 h-4" />
              MANUAL_OVERRIDE
            </CardTitle>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleManualHeal} className="space-y-4">
              <p className="text-xs font-mono text-muted-foreground">
                Force execute a healing strategy on a specific module bypassing automated detection.
              </p>
              <div>
                <label className="text-xs font-mono text-primary uppercase mb-1 block">TARGET_MODULE</label>
                <Input 
                  placeholder="e.g. trading_core_04" 
                  value={moduleInput}
                  onChange={(e) => setModuleInput(e.target.value)}
                  className="bg-background border-primary/50 text-primary-foreground font-mono focus-visible:ring-primary placeholder:text-primary/30"
                />
              </div>
              <Button 
                type="submit" 
                className="w-full bg-primary/20 text-primary border border-primary hover:bg-primary hover:text-primary-foreground transition-all"
                disabled={triggerMutation.isPending || !moduleInput}
              >
                {triggerMutation.isPending ? <Activity className="w-4 h-4 animate-spin mr-2" /> : <Play className="w-4 h-4 mr-2" />}
                EXECUTE_HEAL_ROUTINE
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Errors Table */}
        <Card className="bg-card/40 border-destructive/30">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-destructive tracking-widest flex items-center gap-2">
              <AlertOctagon className="w-4 h-4" />
              ACTIVE_EXCEPTIONS
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-y-auto max-h-[400px]">
              <table className="w-full text-xs font-mono text-left">
                <thead className="bg-destructive/5 text-destructive sticky top-0 border-b border-destructive/30">
                  <tr>
                    <th className="px-4 py-2">MODULE</th>
                    <th className="px-4 py-2">MESSAGE</th>
                    <th className="px-4 py-2">STATUS</th>
                  </tr>
                </thead>
                <tbody>
                  {isErrorsLoading ? (
                    <tr><td colSpan={3} className="px-4 py-4 text-center">Loading...</td></tr>
                  ) : errors?.errors.map((err) => (
                    <tr key={err.id} className="border-b border-destructive/10 hover:bg-destructive/5">
                      <td className="px-4 py-3 text-destructive font-bold">{err.module}</td>
                      <td className="px-4 py-3 text-muted-foreground truncate max-w-[200px]" title={err.message}>
                        {err.message}
                      </td>
                      <td className="px-4 py-3">
                        {err.resolved ? (
                          <CheckCircle2 className="w-4 h-4 text-secondary" />
                        ) : (
                          <Badge variant="outline" className="text-[10px] bg-destructive/20 text-destructive border-destructive/50">OPEN</Badge>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        {/* Heal Actions Table */}
        <Card className="bg-card/40 border-secondary/30">
          <CardHeader>
            <CardTitle className="font-mono text-sm text-secondary tracking-widest flex items-center gap-2">
              <ShieldAlert className="w-4 h-4" />
              REPAIR_LOG
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="overflow-y-auto max-h-[400px]">
              <table className="w-full text-xs font-mono text-left">
                <thead className="bg-secondary/5 text-secondary sticky top-0 border-b border-secondary/30">
                  <tr>
                    <th className="px-4 py-2">TIME</th>
                    <th className="px-4 py-2">MODULE</th>
                    <th className="px-4 py-2">STRATEGY</th>
                    <th className="px-4 py-2">RESULT</th>
                  </tr>
                </thead>
                <tbody>
                  {isActionsLoading ? (
                    <tr><td colSpan={4} className="px-4 py-4 text-center">Loading...</td></tr>
                  ) : actions?.actions.map((act) => (
                    <tr key={act.id} className="border-b border-secondary/10 hover:bg-secondary/5">
                      <td className="px-4 py-3 text-muted-foreground">{format(new Date(act.ts), 'HH:mm:ss')}</td>
                      <td className="px-4 py-3 text-primary-foreground">{act.module}</td>
                      <td className="px-4 py-3 text-accent">{act.strategy}</td>
                      <td className="px-4 py-3">
                        {act.result === 'SUCCESS' ? (
                          <span className="text-secondary text-glow-secondary font-bold">SUCCESS</span>
                        ) : (
                          <span className="text-destructive font-bold">FAILED</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      </div>

    </div>
  );
}
