package beater

import (
    "time"
    "strings"
    "context"

    "github.com/jpfielding/gorets/rets"
    "github.com/elastic/beats/libbeat/logp"
    "fmt"
    "github.com/consulthys/retsbeat/config"
)


type MlsStats struct {
    Code string `json:"code"`
    Resources map[string]map[string]map[string]map[string]int `json:"resources"`
}

type MlsResource struct {
    Name string
    Classes []MlsClass
    StatusField string
    Statuses []Lookup
    TypeField string
    Types []Lookup
}

type MlsClass struct {
    Name string
    Custom []config.Custom
}

type Lookup struct {
    Key string
    Value string
}

func (bt *Retsbeat) GetMetadataResources(sess *RetsSession, statusFields []string, typeFields []string, customQueries []config.Custom) ([]MlsResource, error) {
    // timeout when calling RETS
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
    defer cancel()

    capability, err := rets.Login(sess.Session, ctx, rets.LoginRequest{URL: sess.Config.URL})
    if err != nil {
        return nil, err
    }
    defer rets.Logout(sess.Session, ctx, rets.LogoutRequest{URL: capability.Logout})

    // Get the resources
    metaRes, err := rets.GetCompactMetadata(sess.Session, ctx, rets.MetadataRequest{
        URL:    capability.GetMetadata,
        Format: "COMPACT",
        MType:  "METADATA-RESOURCE",
        ID:     "*",
    })
    if err != nil {
        logp.Err("Error while getting resources: %v", err)
        return nil, err
    }

    mlsRes := make([]MlsResource, 0)
    for _, resDef := range metaRes.Elements["METADATA-RESOURCE"] {
        resIndexer := resDef.Indexer()
        for resIdx, _ := range resDef.Rows {
            mlsRes = append(mlsRes, MlsResource{Name: resIndexer("ResourceID", resIdx)})

            // Map of lookup type per resource status field, e.g.
            // - Property: StatDetail (1_0, 1_1, ...)
            // - Agent: UserActive (Yes, No)
            statusLookups := make(map[string]string)

            // Map of lookup type per resource type field, e.g.
            // - Agent: AgentType (Realtor, Secretary, ...)
            typeLookups := make(map[string]string)

            // Get the classes
            metaCls, err := rets.GetCompactMetadata(sess.Session, ctx, rets.MetadataRequest{
                URL:    capability.GetMetadata,
                Format: "COMPACT",
                MType:  "METADATA-CLASS",
                ID:     mlsRes[resIdx].Name,
            })
            if err != nil {
                logp.Err("Error while getting classes: %v", err)
                return nil, err
            }

            for _, clsDef := range metaCls.Elements["METADATA-CLASS"] {
                clsIndexer := clsDef.Indexer()
                mlsRes[resIdx].Classes = make([]MlsClass, len(clsDef.Rows))
                for clsIdx, _ := range clsDef.Rows {
                    mlsRes[resIdx].Classes[clsIdx] = MlsClass{Name: clsIndexer("ClassName", clsIdx)}

                    // Find custom queries for the resource/class combo
                    queries := make([]config.Custom, 0)
                    for _, custom := range customQueries {
                        if custom.Resource == mlsRes[resIdx].Name && custom.Class == mlsRes[resIdx].Classes[clsIdx].Name {
                            queries = append(queries, custom)
                        }
                    }
                    mlsRes[resIdx].Classes[clsIdx].Custom = queries

                    // Get the class fields table to find the status and type fields
                    resCls := fmt.Sprintf("%v:%v", mlsRes[resIdx].Name, mlsRes[resIdx].Classes[clsIdx].Name)
                    metaCls, err := rets.GetCompactMetadata(sess.Session, ctx, rets.MetadataRequest{
                        URL:    capability.GetMetadata,
                        Format: "COMPACT",
                        MType:  "METADATA-TABLE",
                        ID:     resCls,
                    })
                    if err != nil {
                        logp.Err("Error while getting fields for %v: %v", resCls, err)
                        return nil, err
                    }

                    // Find the status and type fields
                    for _, tableDef := range metaCls.Elements["METADATA-TABLE"] {
                        tableIndexer := tableDef.Indexer()
                        for fieldsIdx, _ := range tableDef.Rows {
                            fieldName := tableIndexer("SystemName", fieldsIdx)

                            // Find the status field
                            for _, statusField := range statusFields {
                                if statusField == fieldName {
                                    mlsRes[resIdx].StatusField = statusField
                                    statusLookups[mlsRes[resIdx].Name] = tableIndexer("LookupName", fieldsIdx)
                                }
                            }

                            // Find the type field
                            for _, typeField := range typeFields {
                                if typeField == fieldName {
                                    mlsRes[resIdx].TypeField = typeField
                                    typeLookups[mlsRes[resIdx].Name] = tableIndexer("LookupName", fieldsIdx)
                                }
                            }
                        }
                    }
                }
            }

            // Get the statuses
            id := []string{mlsRes[resIdx].Name, statusLookups[mlsRes[resIdx].Name]}
            metaStat, err := rets.GetCompactMetadata(sess.Session, ctx, rets.MetadataRequest{
                URL:    capability.GetMetadata,
                Format: "COMPACT",
                MType:  "METADATA-LOOKUP_TYPE",
                ID:     strings.Join(id, ":"),
            })
            if err != nil {
                logp.Err("Error while getting lookup types for %v: %v", strings.Join(id, ":"), err)
                return nil, err
            }
            for _, statDef := range metaStat.Elements["METADATA-LOOKUP_TYPE"] {
                statIndexer := statDef.Indexer()
                mlsRes[resIdx].Statuses = make([]Lookup, len(statDef.Rows))
                for statIdx, _ := range statDef.Rows {
                    mlsRes[resIdx].Statuses[statIdx] = Lookup{
                        Key: statIndexer("Value", statIdx),
                        Value: strings.Replace(statIndexer("LongValue", statIdx), ".", "", -1),
                    }
                }
            }

            // Get the types
            idType := []string{mlsRes[resIdx].Name, typeLookups[mlsRes[resIdx].Name]}
            metaType, err := rets.GetCompactMetadata(sess.Session, ctx, rets.MetadataRequest{
                URL:    capability.GetMetadata,
                Format: "COMPACT",
                MType:  "METADATA-LOOKUP_TYPE",
                ID:     strings.Join(idType, ":"),
            })
            if err != nil {
                logp.Err("Error while getting lookup types for %v: %v", strings.Join(id, ":"), err)
                return nil, err
            }
            for _, typeDef := range metaType.Elements["METADATA-LOOKUP_TYPE"] {
                typeIndexer := typeDef.Indexer()
                mlsRes[resIdx].Types = make([]Lookup, len(typeDef.Rows))
                for typeIdx, _ := range typeDef.Rows {
                    mlsRes[resIdx].Types[typeIdx] = Lookup{
                        Key: typeIndexer("Value", typeIdx),
                        Value: strings.Replace(typeIndexer("LongValue", typeIdx), ".", "", -1),
                    }
                }
            }

            logp.Debug("retsbeat", "%v resource %v has been configured", sess.Code, mlsRes[resIdx].Name)
        }
    }
    return mlsRes, nil
}

func (bt *Retsbeat) GetResourceStats(sess *RetsSession) (*MlsStats, error) {

    // timeout when calling RETS
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
    defer cancel()

    capability, err := rets.Login(sess.Session, ctx, rets.LoginRequest{URL: sess.Config.URL})
    if err != nil {
        return nil, err
    }
    defer rets.Logout(sess.Session, ctx, rets.LogoutRequest{URL: capability.Logout})

    stats := &MlsStats{
        Code: sess.Code,
        Resources: make(map[string]map[string]map[string]map[string]int),
    }
    for r := 0; r < len(sess.Resources); r++ {
        resource := sess.Resources[r]

        stats.Resources[resource.Name] = make(map[string]map[string]map[string]int)
        stats.Resources[resource.Name]["@total"] = make(map[string]map[string]int)

        for c := 0; c < len(resource.Classes); c++ {
            stats.Resources[resource.Name][resource.Classes[c].Name] = make(map[string]map[string]int)

            // Run status queries
            if resource.StatusField != "" {
                stats.Resources[resource.Name][resource.Classes[c].Name]["status"] = make(map[string]int)
                stats.Resources[resource.Name]["@total"]["status"] = make(map[string]int)

                for s := 0; s < len(resource.Statuses); s++ {
                    query := []string{resource.StatusField, resource.Statuses[s].Key}
                    req := rets.SearchRequest{
                        URL:        capability.Search,
                        SearchType: resource.Name,
                        Class:      resource.Classes[c].Name,
                        Query:      fmt.Sprintf("(%v)", strings.Join(query, "=")),
                        Select:     "",
                        Format:     "COMPACT-DECODED",
                        QueryType:  "dmql2",
                        Count:      rets.CountOnly,
                        Limit:      -1,
                    }
                    result, err := rets.SearchCompact(sess.Session, ctx, req)
                    defer result.Close()
                    if err != nil {
                        return nil, err
                    }
                    logp.Debug("retsbeat", "Retrieved count of %v %v / %v with status %v: %v", sess.Code, resource.Name, resource.Classes[c].Name, resource.Statuses[s].Key, result.Count)

                    stats.Resources[resource.Name][resource.Classes[c].Name]["status"][resource.Statuses[s].Value] = result.Count
                    stats.Resources[resource.Name][resource.Classes[c].Name]["status"]["@total"] += result.Count

                    stats.Resources[resource.Name]["@total"]["status"][resource.Statuses[s].Value] += result.Count
                    stats.Resources[resource.Name]["@total"]["status"]["@total"] += result.Count
                }
            }

            // Run type queries
            if resource.TypeField != "" {
                stats.Resources[resource.Name][resource.Classes[c].Name]["types"] = make(map[string]int)
                stats.Resources[resource.Name]["@total"]["types"] = make(map[string]int)

                for s := 0; s < len(resource.Types); s++ {
                    query := []string{resource.TypeField, resource.Types[s].Key}
                    req := rets.SearchRequest{
                        URL:        capability.Search,
                        SearchType: resource.Name,
                        Class:      resource.Classes[c].Name,
                        Query:      fmt.Sprintf("(%v)", strings.Join(query, "=")),
                        Select:     "",
                        Format:     "COMPACT-DECODED",
                        QueryType:  "dmql2",
                        Count:      rets.CountOnly,
                        Limit:      -1,
                    }
                    result, err := rets.SearchCompact(sess.Session, ctx, req)
                    defer result.Close()
                    if err != nil {
                        return nil, err
                    }
                    logp.Debug("retsbeat", "Retrieved count of %v %v / %v with type %v: %v", sess.Code, resource.Name, resource.Classes[c].Name, resource.Types[s].Key, result.Count)

                    stats.Resources[resource.Name][resource.Classes[c].Name]["types"]["@total"] += result.Count
                    stats.Resources[resource.Name][resource.Classes[c].Name]["types"][resource.Types[s].Value] = result.Count

                    stats.Resources[resource.Name]["@total"]["types"][resource.Types[s].Value] += result.Count
                    stats.Resources[resource.Name]["@total"]["types"]["@total"] += result.Count
                }
            }

            // Run custom queries
            if len(resource.Classes[c].Custom) > 0 {
                stats.Resources[resource.Name][resource.Classes[c].Name]["custom"] = make(map[string]int)

                for t := 0; t < len(resource.Classes[c].Custom); t++ {
                    custom := resource.Classes[c].Custom[t]
                    req := rets.SearchRequest{
                        URL:        capability.Search,
                        SearchType: custom.Resource,
                        Class:      custom.Class,
                        Query:      custom.Query,
                        Select:     "",
                        Format:     "COMPACT-DECODED",
                        QueryType:  "dmql2",
                        Count:      rets.CountOnly,
                        Limit:      -1,
                    }
                    result, err := rets.SearchCompact(sess.Session, ctx, req)
                    defer result.Close()
                    if err != nil {
                        return nil, err
                    }
                    logp.Debug("retsbeat", "Retrieved %v custom query %v: %v", sess.Code, custom.Key, result.Count)

                    stats.Resources[custom.Resource][custom.Class]["custom"][custom.Key] = result.Count
                }
            }
        }
    }

    return stats, nil
}
