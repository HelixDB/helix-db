QUERY getUserEmbeddedBio (text: String) =>
    vs <- SearchV<EmbeddedBio>(Embed(text), 10)
    RETURN vs
#[mcp]
QUERY getUserEmbeddedBioMCP (text: String) =>
    vs <- SearchV<EmbeddedBio>(Embed(text), 10)
    RETURN vs

// CRUD Operations
// Create Queries

// User
QUERY createUser (phone: String, email: String, bio: String, age: I32, location: String, profilePic: String, color: String, elo: F64, sender: String) =>
    user <- AddN<User>({phone: phone, email: email, bio: bio, age: age, location: location, profilePic: profilePic, color: color, elo: elo, sender: sender})
    RETURN user

QUERY createMetadata (user_id: ID, created_ts: Date, last_updated_ts: Date, archetype: Boolean, referredBy: String) =>
    metadata <- AddN<Metadata>({created_ts: created_ts, last_updated_ts: last_updated_ts, archetype: archetype, referredBy: referredBy})
    user <- N<User>(user_id)
    user_metadata <- AddE<User_to_Metadata>({created_ts: created_ts, last_updated_ts: last_updated_ts})::From(user)::To(metadata)
    RETURN metadata

QUERY createName (user_id: ID, first: String, last: String) =>
    name <- AddN<Name>({first: first, last: last})
    user <- N<User>(user_id)
    user_name <- AddE<User_to_Name>({first: first})::From(user)::To(name)
    RETURN name

QUERY createMetadataNotes (user_id: ID, aiScore: I64, userScore: I64, text: String, flagged: Boolean) =>
    user_metadata_notes <- AddN<MetadataNotes>({aiScore: aiScore, userScore: userScore, text: text, flagged: flagged})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    metadata_metadata_notes <- AddE<Metadata_to_MetadataNotes>()::From(metadata)::To(user_metadata_notes)
    RETURN user_metadata_notes

// Linkedin
QUERY createLinkedinInfo (user_id: ID, url: String) =>
    linkedin_info <- AddN<LinkedinInfo>({url: url})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    metadata_linkedin_info <- AddE<Metadata_to_LinkedinInfo>({url: url})::From(metadata)::To(linkedin_info)
    RETURN linkedin_info

QUERY createLinkedinContent (user_id: ID, name: String, email: String, linkedin_url: String, full_name: String, first_name: String, last_name: String, public_id: String, profile_picture: String, background_picture: String, current_position: String, summary: String, industry: String, region: String, country: String, country_code: String, connection_count: I64, follower_count: I64, languages: [String], skills: [String], certifications: [String], position_start_date: String, position_end_date: String, extracted_at: Date, data_source: String) =>
    linkedin_content <- AddN<LinkedinContent>({name: name, email: email, linkedin_url: linkedin_url, full_name: full_name, first_name: first_name, last_name: last_name, public_id: public_id, profile_picture: profile_picture, background_picture: background_picture, current_position: current_position, summary: summary, industry: industry, region: region, country: country, country_code: country_code, connection_count: connection_count, follower_count: follower_count, languages: languages, skills: skills, certifications: certifications, position_start_date: position_start_date, position_end_date: position_end_date, extracted_at: extracted_at, data_source: data_source})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content_linkedin_info <- AddE<LinkedinInfo_to_LinkedinContent>()::From(linkedin_info)::To(linkedin_content)
    RETURN linkedin_content

QUERY createLinkedinWebsite (user_id: ID, url: String, category: String) =>
    linkedin_website <- AddN<LinkedinWebsite>({url: url, category: category})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_content_linkedin_website <- AddE<LinkedinContent_to_LinkedinWebsite>()::From(linkedin_content)::To(linkedin_website)
    RETURN linkedin_website

QUERY createLinkedinExperience (user_id: ID, company: String, title: String, field: String, date_start: String, date_end: String, location: String, description: String) =>
    linkedin_experience <- AddN<LinkedinExperience>({company: company, title: title, field: field, date_start: date_start, date_end: date_end, location: location, description: description})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_content_linkedin_experience <- AddE<LinkedinContent_to_LinkedinExperience>()::From(linkedin_content)::To(linkedin_experience)
    RETURN linkedin_experience

QUERY createLinkedinCompany (user_id: ID, name: String, domain: String, industry: String, staff_count: I64, founded: I64, website: String, headquarters: String, description: String, specialties: [String]) =>
    linkedin_company <- AddN<LinkedinCompany>({name: name, domain: domain, industry: industry, staff_count: staff_count, founded: founded, website: website, headquarters: headquarters, description: description, specialties: specialties})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_content_linkedin_company <- AddE<LinkedinContent_to_LinkedinCompany>::From(linkedin_content)::To(linkedin_company)
    RETURN linkedin_company

QUERY addLinkedinCompany (user_id: ID, linkedin_company_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_company <- N<LinkedinCompany>(linkedin_company_id)
    linkedin_content_linkedin_company <- AddE<LinkedinContent_to_LinkedinCompany>()::From(linkedin_content)::To(linkedin_company)
    RETURN linkedin_content_linkedin_company

QUERY createLinkedinEducation (user_id: ID, school: String, field: String, title: String, date_start: String, date_end: String, location: String, description: String) =>
    linkedin_education <- AddN<LinkedinEducation>({school: school, field: field, title: title, date_start: date_start, date_end: date_end, location: location, description: description})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_content_linkedin_education <- AddE<LinkedinContent_to_LinkedinEducation>()::From(linkedin_content)::To(linkedin_education)
    RETURN linkedin_education

// Warm Connect
QUERY createWarmConnect (user_id: ID, name: String, email: String) =>
    warm_connect <- AddN<WarmConnect>({name: name, email: email})
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    metadata_to_warm_connect <- AddE<Metadata_to_WarmConnect>()::From(metadata)::To(warm_connect)
    RETURN warm_connect

QUERY addWarmConnect (user_id: ID, warm_connect_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    warm_connect <- N<WarmConnect>(warm_connect_id)
    metadata_to_warm_connect <- AddE<Metadata_to_WarmConnect>()::From(metadata)::To(warm_connect)
    RETURN warm_connect

QUERY createUserBio (user_id: ID, bio: [F64]) =>
    user_bio <- AddV<EmbeddedBio>(bio)
    user <- N<User>(user_id)
    user_user_bio <- AddE<User_to_EmbeddedBio>()::From(user)::To(user_bio)
    RETURN user_bio

QUERY addCurrentCompany (user_id: ID, linkedin_company_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_company <- N<LinkedinCompany>(linkedin_company_id)
    linkedin_content_current_company <- AddE<LinkedinContent_to_CurrentCompany>()::From(linkedin_content)::To(linkedin_company)
    RETURN linkedin_content_current_company

// Read Queries

// User
#[mcp]
QUERY getUser (user_id: ID) =>
    user <- N<User>(user_id)
    RETURN user

#[mcp]
QUERY getUserName (user_id: ID) =>
    user <- N<User>(user_id)
    name <- user::Out<User_to_Name>
    RETURN name

#[mcp]
QUERY getAllUsers() =>
    users <- N<User>
    RETURN users

#[mcp]
QUERY getEmbedUserBio (user_id: ID) =>
    user <- N<User>(user_id)
    user_bio <- user::Out<User_to_EmbeddedBio>
    RETURN user_bio

#[mcp]
QUERY getUsersByReferrer(referrer: String) =>
    metadata <- N<Metadata>::WHERE(_::{referredBy}::EQ(referrer))
    users <- metadata::In<User_to_Metadata>
    RETURN users

#[mcp]
QUERY searchUsersByBio(bio_vector: [F64], k: I64) =>
    similar_bios <- SearchV<EmbeddedBio>(bio_vector, k)
    users <- similar_bios::In<User_to_EmbeddedBio>
    RETURN users

// Metadata
#[mcp]
QUERY getUserMetadata (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    RETURN metadata

#[mcp]
QUERY getUserMetadataNotes (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    metadata_notes <- metadata::Out<Metadata_to_MetadataNotes>
    RETURN metadata_notes

// Linkedin
#[mcp]
QUERY getUserLinkedinInfo (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    RETURN linkedin_info

#[mcp]
QUERY getUserLinkedinContent (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    RETURN linkedin_content

#[mcp]
QUERY getUserLinkedinWebsites (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_websites <- linkedin_content::Out<LinkedinContent_to_LinkedinWebsite>
    RETURN linkedin_websites

#[mcp]
QUERY getUserLinkedinExperiences (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_experiences <- linkedin_content::Out<LinkedinContent_to_LinkedinExperience>
    RETURN linkedin_experiences

#[mcp]
QUERY getUserLinkedinCompanies (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_companies <- linkedin_content::Out<LinkedinContent_to_LinkedinCompany>
    RETURN linkedin_companies

#[mcp]
QUERY getUserLinkedinEducations (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    linkedin_educations <- linkedin_content::Out<LinkedinContent_to_LinkedinEducation>
    RETURN linkedin_educations

#[mcp]
QUERY getUserLinkedinCurrentCompany (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    linkedin_info <- metadata::Out<Metadata_to_LinkedinInfo>
    linkedin_content <- linkedin_info::Out<LinkedinInfo_to_LinkedinContent>
    current_company <- linkedin_content::Out<LinkedinContent_to_CurrentCompany>
    RETURN current_company

#[mcp]
QUERY findLinkedinCompany (name: String, industry: String, founded: I64) =>
    linkedin_company <- N<LinkedinCompany>::WHERE(
        AND(
            _::{name}::EQ(name),
            _::{industry}::EQ(industry),
            _::{founded}::EQ(founded)
        )
    )
    RETURN linkedin_company

// Warm Connect
#[mcp]
QUERY getUserWarmConnects (user_id: ID) =>
    user <- N<User>(user_id)
    metadata <- user::Out<User_to_Metadata>
    warm_connects <- metadata::Out<Metadata_to_WarmConnect>
    RETURN warm_connects

#[mcp]
QUERY findWarmConnect (name: String, email: String) =>
    warm_connect <- N<WarmConnect>::WHERE(
        AND(
            _::{name}::EQ(name),
            _::{email}::EQ(email)
        )
    )
    RETURN warm_connect

// Update Queries

// User
QUERY updateUser (user_id: ID, phone: String, email: String, bio: String, age: I32, location: String, profilePic: String, color: String, elo: F64, sender: String) =>
    user <- N<User>(user_id)::UPDATE(
        {phone: phone, email: email, bio: bio, age: age, location: location, profilePic: profilePic, color: color, elo: elo, sender: sender}
    )
    RETURN user

QUERY updateUserName (name_id: ID, first: String, last: String) =>
    name <- N<Name>(name_id)::UPDATE(
        {first: first, last: last}
    )
    RETURN name

// Metadata
QUERY updateMetadata (metadata_id: ID, created_ts: Date, last_updated_ts: Date, archetype: Boolean, referredBy: String) =>
    metadata <- N<Metadata>(metadata_id)::UPDATE(
        {created_ts: created_ts, last_updated_ts: last_updated_ts, archetype: archetype, referredBy: referredBy}
    )
    RETURN metadata

QUERY updateMetadataNotes (metadata_notes_id: ID, aiScore: I64, userScore: I64, text: String, flagged: Boolean) =>
    metadata_notes <- N<MetadataNotes>(metadata_notes_id)::UPDATE(
        {aiScore: aiScore, userScore: userScore, text: text, flagged: flagged}
    )
    RETURN metadata_notes

// Linkedin
QUERY updateLinkedinInfo (linkedin_info_id: ID, url: String) =>
    linkedin_info <- N<LinkedinInfo>(linkedin_info_id)::UPDATE(
        {url: url}
    )
    RETURN linkedin_info

QUERY updateLinkedinContent (linkedin_content_id: ID, name: String, email: String, linkedin_url: String, full_name: String, first_name: String, last_name: String, public_id: String, profile_picture: String, background_picture: String, current_position: String, summary: String, industry: String, region: String, country: String, country_code: String, connection_count: I64, follower_count: I64, languages: [String], skills: [String], certifications: [String], position_start_date: String, position_end_date: String, extracted_at: Date, data_source: String) =>
    linkedin_content <- N<LinkedinContent>(linkedin_content_id)::UPDATE(
        {name: name, email: email, linkedin_url: linkedin_url, full_name: full_name, first_name: first_name, last_name: last_name, public_id: public_id, profile_picture: profile_picture, background_picture: background_picture, current_position: current_position, summary: summary, industry: industry, region: region, country: country, country_code: country_code, connection_count: connection_count, follower_count: follower_count, languages: languages, skills: skills, certifications: certifications, position_start_date: position_start_date, position_end_date: position_end_date, extracted_at: extracted_at, data_source: data_source}
    )
    RETURN linkedin_content

QUERY updateLinkedinWebsite (website_id: ID, url: String, category: String) =>
    linkedin_website <- N<LinkedinWebsite>(website_id)::UPDATE(
        {url: url, category: category}
    )
    RETURN linkedin_website

QUERY updateLinkedinExperience (experience_id: ID, company: String, title: String, field: String, date_start: String, date_end: String, location: String, description: String) =>
    linkedin_experience <- N<LinkedinExperience>(experience_id)::UPDATE(
        {company: company, title: title, field: field, date_start: date_start, date_end: date_end, location: location, description: description}
    )
    RETURN linkedin_experience

QUERY updateLinkedinCompany (company_id: ID, name: String, domain: String, industry: String, staff_count: I64, founded: I64, website: String, headquarters: String, description: String, specialties: [String]) =>
    linkedin_company <- N<LinkedinCompany>(company_id)::UPDATE(
        {name: name, domain: domain, industry: industry, staff_count: staff_count, founded: founded, website: website, headquarters: headquarters, description: description, specialties: specialties}
    )
    RETURN linkedin_company

QUERY updateLinkedinEducation (education_id: ID, school: String, field: String, title: String, date_start: String, date_end: String, location: String, description: String) =>
    linkedin_education <- N<LinkedinEducation>(education_id)::UPDATE(
        {school: school, field: field, title: title, date_start: date_start, date_end: date_end, location: location, description: description}
    )
    RETURN linkedin_education

QUERY updateCurrentCompany (linkedin_company_id: ID, name: String, domain: String, industry: String, staff_count: I64, founded: I64, website: String, headquarters: String, description: String, specialties: [String]) =>
    linkedin_company <- N<LinkedinCompany>(linkedin_company_id)::UPDATE(
        {name: name, domain: domain, industry: industry, staff_count: staff_count, founded: founded, website: website, headquarters: headquarters, description: description, specialties: specialties}
    )
    RETURN linkedin_company

// Warm Connect
QUERY updateWarmConnect (warm_connect_id: ID, name: String, email: String) =>
    warm_connect <- N<WarmConnect>(warm_connect_id)::UPDATE(
        {name: name, email: email}
    )
    RETURN warm_connect

// Delete Queries
QUERY deleteLinkedinEducation (education_id: ID) =>
    DROP N<LinkedinEducation>(education_id)
    RETURN "success"

QUERY deleteLinkedinCompany (company_id: ID) =>
    DROP N<LinkedinCompany>(company_id)
    RETURN "success"

QUERY deleteLinkedinExperience (experience_id: ID) =>
    DROP N<LinkedinExperience>(experience_id)
    RETURN "success"

QUERY deleteLinkedinWebsite (website_id: ID) =>
    DROP N<LinkedinWebsite>(website_id)
    RETURN "success"

QUERY deleteLinkedinContent (linkedin_content_id: ID) =>
    DROP N<LinkedinContent>(linkedin_content_id)::Out<LinkedinContent_to_LinkedinWebsite>
    DROP N<LinkedinContent>(linkedin_content_id)::Out<LinkedinContent_to_LinkedinExperience>
    DROP N<LinkedinContent>(linkedin_content_id)::OutE<LinkedinContent_to_LinkedinCompany>
    DROP N<LinkedinContent>(linkedin_content_id)::Out<LinkedinContent_to_LinkedinEducation>
    DROP N<LinkedinContent>(linkedin_content_id)::OutE<LinkedinContent_to_CurrentCompany>
    DROP N<LinkedinContent>(linkedin_content_id)
    RETURN "success"

QUERY deleteLinkedinInfo (linkedin_info_id: ID) =>
    DROP N<LinkedinInfo>(linkedin_info_id)::Out<LinkedinInfo_to_LinkedinContent>
    DROP N<LinkedinInfo>(linkedin_info_id)
    RETURN "success"

QUERY deleteWarmConnect (warm_connect_id: ID) =>
    DROP N<WarmConnect>(warm_connect_id)
    RETURN "success"

QUERY deleteMetadataNotes (metadata_notes_id: ID) =>
    DROP N<MetadataNotes>(metadata_notes_id)
    RETURN "success"

QUERY deleteMetadata (metadata_id: ID) =>
    DROP N<Metadata>(metadata_id)::Out<Metadata_to_MetadataNotes>
    DROP N<Metadata>(metadata_id)::Out<Metadata_to_LinkedinInfo>
    DROP N<Metadata>(metadata_id)::Out<Metadata_to_WarmConnect>
    DROP N<Metadata>(metadata_id)
    RETURN "success"

QUERY deleteEmbeddedBio (user_id: ID) =>
    DROP N<User>(user_id)::OutE<User_to_EmbeddedBio>
    RETURN "success"

QUERY deleteName (name_id: ID) =>
    DROP N<Name>(name_id)
    RETURN "success"

QUERY deleteUser (user_id: ID) =>
    DROP N<User>(user_id)::Out<User_to_Name>
    DROP N<User>(user_id)::Out<User_to_Metadata>
    DROP N<User>(user_id)::OutE<User_to_EmbeddedBio>
    DROP N<User>(user_id)
    RETURN "success"