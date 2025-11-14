N::User {
    name: String,
    email: String,
    password: String,
}
 
N::App {
    name: String DEFAULT "Some",
    description: String DEFAULT "",
    created_at: Date,
    favorite: Boolean,
    archived: Boolean,
}
 
N::Database {
 
}
 
E::AppHasProdDb {
    From: App,
    To: Database,
    Properties: {
        created_at: Date,
      }
  }
N::DbConnection {
   host: String,
   port: String,
   username: String,
   password: String,
   name: String,
   ssl: String,
}
 
E::DbHasDbConnection {
    From: Database,
    To: DbConnection,
    Properties: {
        created_at: Date,
      }
  }
 
 
N::Branch {
    name: String,
}
E::BranchHasDbConnection {
    From: Branch,
    To: Database,
    Properties: {
        created_at: Date,
      }
  }
N::FolderItem {
 
  }
N::Frontend {
}
 
N::Backend {
}
 
N::Element {
    element_id: String,
    name: String,
}
 
N::PageFolder {
    name: String,
}
 
N::Page {
    name: String,
}
 
N::FolderTree {
    name: String,
    description: String,
}
 
E::Backend_Has_Root_Folder {
    From: Backend,
    To: FolderTree,
    Properties: {
        created_at: Date,
    }
}
 
E::FolderItem_Has_FolderTree {
    From: FolderItem,
    To: FolderTree,
    Properties: {
        created_at: Date,
      }
  }
E::App_Has_Branch {
    From: App,
    To: Branch,
    Properties: {
        created_at: Date,
    }
}
 
E::Branch_Has_Frontend {
    From: Branch,
    To: Frontend,
    Properties: {
        created_at: Date,
    }
}
 
E::Branch_Has_Backend {
    From: Branch,
    To: Backend,
    Properties: {
        created_at: Date,
    }
}
 
E::Frontend_Contains_PageFolder {
    From: Frontend,
    To: PageFolder,
    Properties: {
        created_at: Date,
        assigned_at: Date,
    }
}
 
E::Page_Has_Root_Element {
    From: Page,
    To: Element,
    Properties: {
        created_at: Date,
        assigned_at: Date,
    }
}
 
E::Frontend_Has_Page {
    From: Frontend,
    To: Page,
    Properties: {
        created_at: Date,
        assigned_at: Date,
    }
}
 
E::PageFolder_Contains_Page {
    From: PageFolder,
    To: Page,
    Properties: {
        created_at: Date,
        assigned_at: Date,
    }
}
 
E::User_Has_Access_To {
    From: User,
    To: App,
    Properties: {
        created_at: Date,
        modified_at: Date,
    }
}