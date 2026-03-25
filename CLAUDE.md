# Tandem Protocol
@~/projects/tandem-protocol/README.md

# toc

### evtctl — project task management

```
evtctl task <description>            # publish a task event
evtctl task --to <project> <desc>    # task for another project
evtctl inbox <app> <message>         # send inbox message
evtctl done <id>[,<id>...] [evidence] # publish a task-done event
evtctl open                          # list open tasks
evtctl audit                         # full task reconciliation
evtctl claim <id> <name>             # claim a task
evtctl claims                        # list active claims
```

Stream name automatically derived from project directory: `tasks.toc`. To send tasks to other projects: `evtctl task --to <project> <description>`. To send inbox messages: `evtctl inbox <app> <message>`.
