
class SalesforceQueries:
    """
    This class contains base queries for Salesforce objects.
    These queries can be used to fetch data from Salesforce.
    """
    def __init__(self):
        self.base_query_opportunities = """
        SELECT
          Id, AccountId, IsPrivate, Name, Description, StageName,
          Amount, Probability, ExpectedRevenue, TotalOpportunityQuantity, CloseDate,
          Type, LeadSource, IsClosed, IsWon, ForecastCategory, ForecastCategoryName,
          OwnerId,  Owner.Name,  CreatedDate,  CreatedById,  LastModifiedDate,  LastModifiedById,
          PushCount, LastStageChangeDate, FiscalQuarter, FiscalYear, Fiscal, ContactId, LastViewedDate, LastReferencedDate,
          HasOpenActivity, HasOverdueTask, Owner.Email
        FROM Opportunity
        """
        self.base_query_accounts = """
        SELECT
          Id, Name, Type, BillingStreet, BillingCity,
          BillingState, BillingPostalCode, BillingCountry, ShippingStreet, ShippingCity,
          ShippingState, ShippingPostalCode, ShippingCountry, Phone, Fax, AccountNumber,
          Website, PhotoUrl, Sic, Industry, AnnualRevenue,
          NumberOfEmployees, Ownership, TickerSymbol, Description, Rating, OwnerId, CleanStatus,
          AccountSource, DunsNumber,  CreatedDate, CreatedById, LastModifiedDate, LastModifiedById
        FROM Account

        """
        self.base_query_contacts="""
SELECT
  Id, IsDeleted, MasterRecordId, AccountId, LastName, FirstName,
  Salutation, Name, OtherStreet, OtherCity, OtherState, OtherPostalCode, OtherCountry,
  MailingStreet, MailingCity, MailingState, MailingPostalCode, MailingCountry, Phone,
  Fax, MobilePhone, HomePhone, OtherPhone, AssistantPhone, Email, Title, Department, AssistantName,
  LeadSource,Description,OwnerId,CreatedDate,LastModifiedDate, LastActivityDate, LastCURequestDate, LastCUUpdateDate,
  LastViewedDate,LastReferencedDate,EmailBouncedReason,EmailBouncedDate,
  IsEmailBounced, PhotoUrl, Jigsaw, JigsawContactId, CleanStatus,
  IndividualId
FROM Contact
"""
        self.base_query_opp_Activities = """
SELECT
  Id, AccountId, WhatId, WhoId, Subject, ActivityDate, Status,
  Priority, OwnerId, CreatedDate, IsClosed, IsDeleted, CallType,
  CallDisposition, CallObject, CreatedById, Description, CompletedDateTime, TaskSubType
FROM Task
WHERE WhatId IN (
  SELECT Id FROM Opportunity
)
"""
        self.base_query_opp_history = """
SELECT
  Id, OpportunityId, CreatedDate, Amount, PrevAmount,
  CloseDate, PrevCloseDate, ExpectedRevenue, Probability, StageName,
  ForecastCategory,  IsDeleted
FROM OpportunityHistory
"""
        self.base_query_users = """
SELECT 
    Id, Username, LastName, FirstName, Name, CompanyName, Division,
    Department, Title, Street, City, State, PostalCode, Country,
    Email, EmailPreferencesAutoBcc, EmailPreferencesAutoBccStayInTouch, EmailPreferencesStayInTouchReminder,
    SenderEmail,SenderName,MobilePhone,Alias, CommunityNickname, BadgeText,
    IsActive, TimeZoneSidKey, UserRoleId, LocaleSidKey, ReceivesInfoEmails,
    ReceivesAdminInfoEmails, EmailEncodingKey, ProfileId, UserType, LanguageLocaleKey,
    EmployeeNumber, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, AboutMe
FROM User
"""
        self.query_for_callstage="""
SELECT Id, MasterLabel, IsActive, SortOrder FROM OpportunityStage
"""