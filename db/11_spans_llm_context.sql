                                                            
                                                                              
                                                                        
                                                                           
                                                                            

ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_user_id        String                        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_session_id     String                        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_tags           Array(LowCardinality(String)) CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_release        LowCardinality(String)        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_prompt_name    LowCardinality(String)        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_prompt_version UInt32                        CODEC(T64, ZSTD(1));
                                                                          
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS gen_ai_span_kind   LowCardinality(String)        CODEC(ZSTD(1));
