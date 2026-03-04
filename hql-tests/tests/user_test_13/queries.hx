N::Person{

}
    
E::SpouseOf {                                                                 
      From: Person,                                                             
      To: Person,                                                               
      Properties: {                                                             
          marriageDate: Date,                                                   
          divorceDate: Date,                                                    
          status: String,                                                       
          createdAt: Date DEFAULT NOW,                                          
      }                                                                         
  }                                                                             
          
  
  QUERY updateSpouseRelationship(                                               
      person1Id: ID,                                                            
      person2Id: ID,                                                            
      marriageDate: Date,                                                       
      status: String                                                            
  ) =>                                                                          
      edges1 <-                                                                 
  N<Person>(person1Id)::OutE<SpouseOf>::WHERE(_::ToN::ID::EQ(person2Id))        
      updated_edge1 <- edges1::UPDATE({                                         
          marriageDate: marriageDate,                                           
          status: status                                                        
      })                                                                        
      edges2 <-                                                                 
  N<Person>(person2Id)::OutE<SpouseOf>::WHERE(_::ToN::ID::EQ(person1Id))        
      updated_edge2 <- edges2::UPDATE({                                         
          marriageDate: marriageDate,                                           
          status: status                                                        
      })                                                                        
      RETURN updated_edge1                                                      
                                                                                
                                                                                                         
                                                                                
